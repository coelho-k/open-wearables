"""Fitbit 247 data sync: sleep, daily activity, HRV, blood/respiratory,
body composition, and fitness metrics."""

import logging
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any
from uuid import UUID, uuid4
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from app.database import DbSession
from app.repositories import UserConnectionRepository
from app.schemas import EventRecordCreate, TimeSeriesSampleCreate
from app.schemas.event_record_detail import EventRecordDetailCreate
from app.schemas.series_types import SeriesType
from app.services.event_record_service import event_record_service
from app.services.providers.api_client import make_authenticated_request
from app.services.providers.templates.base_oauth import BaseOAuthTemplate
from app.services.timeseries_service import timeseries_service
from app.utils.sentry_helpers import log_and_capture_error


class FitbitData:
    """Fitbit pull-based 247 data handler.

    Syncs sleep, daily activity, HRV, SpO2, breathing rate,
    body composition, and VO2 max from the Fitbit Web API.
    No base class — the Base247DataTemplate ABC uses a fetch/normalize/process split that does not map
    to Fitbit's flat load_and_save_all surface. Celery discovers this class via hasattr duck-typing.
    """

    def __init__(
        self,
        provider_name: str,
        api_base_url: str,
        oauth: BaseOAuthTemplate,
    ) -> None:
        self.provider_name = provider_name
        self.api_base_url = api_base_url
        self.oauth = oauth
        self.connection_repo = UserConnectionRepository()
        self.logger = logging.getLogger(self.__class__.__name__)

    def _make_api_request(
        self,
        db: DbSession,
        user_id: UUID,
        endpoint: str,
        params: dict[str, Any] | None = None,
    ) -> Any:
        """Make an authenticated GET request to the Fitbit API."""
        return make_authenticated_request(
            db=db,
            user_id=user_id,
            connection_repo=self.connection_repo,
            oauth=self.oauth,
            api_base_url=self.api_base_url,
            provider_name=self.provider_name,
            endpoint=endpoint,
            method="GET",
            params=params,
        )

    def _get_user_timezone(self, db: DbSession, user_id: UUID) -> ZoneInfo | timezone:
        """Fetch the user's timezone from the Fitbit profile API.

        Returns a ZoneInfo for the user's local timezone, falling back to UTC on any error.
        """
        try:
            profile = self._make_api_request(db, user_id, "/1/user/-/profile.json")
            tz_name = (profile or {}).get("user", {}).get("timezone", "UTC")
            return ZoneInfo(tz_name)
        except ZoneInfoNotFoundError:
            self.logger.warning("Unknown Fitbit timezone '%s'; falling back to UTC", tz_name)
            return timezone.utc
        except Exception as e:
            log_and_capture_error(
                e, self.logger, "Fitbit: failed to fetch user timezone", extra={"user_id": str(user_id)}
            )
            return timezone.utc

    def load_sleep(
        self,
        db: DbSession,
        user_id: UUID,
        start_dt: datetime,
        end_dt: datetime,
    ) -> int:
        """Fetch sleep sessions from Fitbit and save as EventRecord + EventRecordDetail.

        Returns the number of sessions saved.
        """
        # Fitbit returns startTime/endTime as local time strings without timezone info.
        # Fetch the user's timezone once so we can convert to UTC correctly.
        user_tz = self._get_user_timezone(db, user_id)

        params: dict[str, Any] = {
            "afterDate": start_dt.strftime("%Y-%m-%d"),
            "sort": "asc",
            "limit": "100",
            "offset": "0",
        }
        end_date_str = end_dt.strftime("%Y-%m-%d")
        offset = 0
        count = 0

        while True:
            params["offset"] = str(offset)
            response = self._make_api_request(db, user_id, "/1.2/user/-/sleep/list.json", params=params)
            if not response:
                break

            sessions = response.get("sleep", [])
            in_range = [s for s in sessions if (s.get("startTime") or "")[:10] <= end_date_str]

            for raw in in_range:
                try:
                    record_id = uuid4()
                    start_time_str: str = raw["startTime"]
                    end_time_str: str = raw["endTime"]
                    # Fitbit normally returns naive local-time strings (no TZ offset).
                    # Attach the user's timezone before converting to UTC.
                    # If a TZ offset is already present (edge case), use it directly.
                    start_parsed = datetime.fromisoformat(start_time_str)
                    end_parsed = datetime.fromisoformat(end_time_str)
                    start_datetime = (
                        start_parsed.replace(tzinfo=user_tz).astimezone(timezone.utc)
                        if start_parsed.tzinfo is None
                        else start_parsed.astimezone(timezone.utc)
                    )
                    end_datetime = (
                        end_parsed.replace(tzinfo=user_tz).astimezone(timezone.utc)
                        if end_parsed.tzinfo is None
                        else end_parsed.astimezone(timezone.utc)
                    )
                    duration_seconds = raw["duration"] // 1000

                    levels_summary = raw.get("levels", {}).get("summary", {})
                    deep_minutes = levels_summary.get("deep", {}).get("minutes", 0)
                    light_minutes = levels_summary.get("light", {}).get("minutes", 0)
                    rem_minutes = levels_summary.get("rem", {}).get("minutes", 0)
                    wake_minutes = levels_summary.get("wake", {}).get("minutes", 0)
                    total_sleep_minutes = deep_minutes + light_minutes + rem_minutes

                    efficiency_raw = raw.get("efficiency")

                    record = EventRecordCreate(
                        id=record_id,
                        category="sleep",
                        type="sleep_session",
                        source_name="Fitbit",
                        device_model=None,
                        duration_seconds=duration_seconds,
                        start_datetime=start_datetime,
                        end_datetime=end_datetime,
                        external_id=str(raw["logId"]),
                        source=self.provider_name,
                        user_id=user_id,
                    )
                    detail = EventRecordDetailCreate(
                        record_id=record_id,
                        sleep_total_duration_minutes=total_sleep_minutes,
                        sleep_time_in_bed_minutes=duration_seconds // 60,
                        sleep_efficiency_score=Decimal(str(efficiency_raw)) if efficiency_raw is not None else None,
                        sleep_deep_minutes=deep_minutes,
                        sleep_light_minutes=light_minutes,
                        sleep_rem_minutes=rem_minutes,
                        sleep_awake_minutes=wake_minutes,
                        is_nap=not raw.get("isMainSleep", True),
                    )

                    created_record = event_record_service.create(db, record)
                    # Use the persisted id: if a record with this external_id already exists, the
                    # repository returns it — so created_record.id may differ from the locally-generated uuid.
                    detail_for_record = detail.model_copy(update={"record_id": created_record.id})
                    event_record_service.create_detail(db, detail_for_record, detail_type="sleep")
                    count += 1
                except Exception as e:
                    log_and_capture_error(
                        e,
                        self.logger,
                        f"Fitbit sleep: failed to save session {raw.get('logId', 'unknown')}",
                        extra={"user_id": str(user_id), "log_id": str(raw.get("logId"))},
                    )

            next_page = response.get("pagination", {}).get("next", "")
            if not next_page or not sessions or len(in_range) < len(sessions):
                break
            offset += len(sessions)

        return count

    def load_daily_activity(
        self,
        db: DbSession,
        user_id: UUID,
        start_dt: datetime,
        end_dt: datetime,
    ) -> int:
        """Fetch daily activity summaries (steps, calories, HR, etc.) and save as TimeSeriesSamples.

        One API request per calendar day. Both activity metrics and resting heart rate
        are extracted from the same response.
        Returns the number of samples saved.
        """
        count = 0
        current = start_dt.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc)
        end_day = end_dt.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc)

        while current <= end_day:
            date_str = current.strftime("%Y-%m-%d")
            recorded_at = datetime(current.year, current.month, current.day, tzinfo=timezone.utc)

            try:
                response = self._make_api_request(
                    db,
                    user_id,
                    f"/1/user/-/activities/date/{date_str}.json",
                )
                if not response:
                    current += timedelta(days=1)
                    continue

                summary = response.get("summary", {})

                # Build (field_name, SeriesType, value) tuples; skip if value absent
                metrics: list[tuple[str, SeriesType, Any]] = []

                steps = summary.get("steps")
                if steps is not None:
                    metrics.append(("steps", SeriesType.steps, steps))

                calories_out = summary.get("caloriesOut")
                if calories_out is not None:
                    metrics.append(("caloriesOut", SeriesType.energy, calories_out))

                calories_bmr = summary.get("caloriesBMR")
                if calories_bmr is not None:
                    metrics.append(("caloriesBMR", SeriesType.basal_energy, calories_bmr))

                floors = summary.get("floors")
                if floors is not None:
                    metrics.append(("floors", SeriesType.flights_climbed, floors))

                fairly = summary.get("fairlyActiveMinutes")
                very = summary.get("veryActiveMinutes")
                if fairly is not None or very is not None:
                    metrics.append(("exercise_time", SeriesType.exercise_time, (fairly or 0) + (very or 0)))

                resting_hr = summary.get("restingHeartRate")
                if resting_hr is not None:
                    metrics.append(("restingHeartRate", SeriesType.resting_heart_rate, resting_hr))

                # Heart rate zones: Fitbit pre-computes minutes in Fat Burn / Cardio / Peak zones.
                # Map these to HR zone series types so the UI can display them as intensity_minutes.
                for zone in summary.get("heartRateZones", []):
                    zone_name = zone.get("name", "").lower()
                    zone_minutes = zone.get("minutes")
                    if zone_minutes is None:
                        continue
                    if "fat burn" in zone_name:
                        metrics.append(("hr_zone_fat_burn", SeriesType.hr_zone_fat_burn, zone_minutes))
                    elif "cardio" in zone_name:
                        metrics.append(("hr_zone_cardio", SeriesType.hr_zone_cardio, zone_minutes))
                    elif "peak" in zone_name:
                        metrics.append(("hr_zone_peak", SeriesType.hr_zone_peak, zone_minutes))

                # Distance: find the "total" entry and convert km → meters
                # Note: distances lives inside summary, not at the top-level response.
                for dist_entry in summary.get("distances", []):
                    if dist_entry.get("activity") == "total":
                        dist_km = dist_entry.get("distance")
                        if dist_km is not None:
                            dist_m = Decimal(str(dist_km)) * 1000
                            metrics.append(("distance", SeriesType.distance_walking_running, dist_m))
                        break

                for field_name, series_type, value in metrics:
                    try:
                        sample = TimeSeriesSampleCreate(
                            id=uuid4(),
                            user_id=user_id,
                            source=self.provider_name,
                            recorded_at=recorded_at,
                            value=Decimal(str(value)),
                            series_type=series_type,
                        )
                        timeseries_service.crud.create(db, sample)
                        count += 1
                    except Exception as e:
                        log_and_capture_error(
                            e,
                            self.logger,
                            f"Fitbit daily_activity: failed to save {field_name} for {date_str}",
                            extra={"user_id": str(user_id), "date": date_str, "field": field_name},
                        )

            except Exception as e:
                log_and_capture_error(
                    e,
                    self.logger,
                    f"Fitbit daily_activity: failed to fetch data for {date_str}",
                    extra={"user_id": str(user_id), "date": date_str},
                )

            current += timedelta(days=1)

        return count

    @staticmethod
    def _date_range_chunks(start_dt: datetime, end_dt: datetime, max_days: int = 30) -> list[tuple[str, str]]:
        """Split a date range into chunks of at most max_days days.

        Returns a list of (start_str, end_str) pairs in 'YYYY-MM-DD' format.
        """
        chunks: list[tuple[str, str]] = []
        chunk_start = start_dt.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc)
        range_end = end_dt.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc)

        while chunk_start <= range_end:
            chunk_end = min(chunk_start + timedelta(days=max_days - 1), range_end)
            chunks.append((chunk_start.strftime("%Y-%m-%d"), chunk_end.strftime("%Y-%m-%d")))
            chunk_start = chunk_end + timedelta(days=1)

        return chunks

    def load_hrv(
        self,
        db: DbSession,
        user_id: UUID,
        start_dt: datetime,
        end_dt: datetime,
    ) -> int:
        """Fetch daily HRV (RMSSD) and save as TimeSeriesSamples.

        Chunked into 30-day windows due to Fitbit API limit.
        Returns the number of samples saved.
        """
        count = 0

        for start_str, end_str in self._date_range_chunks(start_dt, end_dt):
            try:
                response = self._make_api_request(
                    db,
                    user_id,
                    f"/1/user/-/hrv/date/{start_str}/{end_str}.json",
                )
                if not response:
                    continue

                for item in response.get("hrv", []):
                    try:
                        date_str: str = item["dateTime"]
                        parsed = datetime.strptime(date_str, "%Y-%m-%d")
                        recorded_at = datetime(parsed.year, parsed.month, parsed.day, tzinfo=timezone.utc)
                        daily_rmssd = item["value"]["dailyRmssd"]

                        sample = TimeSeriesSampleCreate(
                            id=uuid4(),
                            user_id=user_id,
                            source=self.provider_name,
                            recorded_at=recorded_at,
                            value=Decimal(str(daily_rmssd)),
                            series_type=SeriesType.heart_rate_variability_rmssd,
                        )
                        timeseries_service.crud.create(db, sample)
                        count += 1
                    except Exception as e:
                        log_and_capture_error(
                            e,
                            self.logger,
                            f"Fitbit HRV: failed to save item {item.get('dateTime', 'unknown')}",
                            extra={"user_id": str(user_id), "date": item.get("dateTime")},
                        )
            except Exception as e:
                log_and_capture_error(
                    e,
                    self.logger,
                    f"Fitbit HRV: failed to fetch chunk {start_str}–{end_str}",
                    extra={"user_id": str(user_id), "chunk_start": start_str, "chunk_end": end_str},
                )

        return count

    def load_blood_respiratory(
        self,
        db: DbSession,
        user_id: UUID,
        start_dt: datetime,
        end_dt: datetime,
    ) -> int:
        """Fetch SpO2 and breathing rate and save as TimeSeriesSamples.

        Chunked into 30-day windows due to Fitbit API limit.
        Returns the number of samples saved.
        """
        count = 0

        for start_str, end_str in self._date_range_chunks(start_dt, end_dt):
            # --- SpO2 ---
            try:
                spo2_response = self._make_api_request(
                    db,
                    user_id,
                    f"/1/user/-/spo2/date/{start_str}/{end_str}.json",
                )
                if isinstance(spo2_response, list):
                    for item in spo2_response:
                        try:
                            value_obj = item.get("value", {})
                            avg = value_obj.get("avg")
                            if avg is None:
                                continue
                            date_str: str = item["dateTime"]
                            parsed = datetime.strptime(date_str, "%Y-%m-%d")
                            recorded_at = datetime(parsed.year, parsed.month, parsed.day, tzinfo=timezone.utc)

                            sample = TimeSeriesSampleCreate(
                                id=uuid4(),
                                user_id=user_id,
                                source=self.provider_name,
                                recorded_at=recorded_at,
                                value=Decimal(str(avg)),
                                series_type=SeriesType.oxygen_saturation,
                            )
                            timeseries_service.crud.create(db, sample)
                            count += 1
                        except Exception as e:
                            log_and_capture_error(
                                e,
                                self.logger,
                                f"Fitbit SpO2: failed to save item {item.get('dateTime', 'unknown')}",
                                extra={"user_id": str(user_id), "date": item.get("dateTime")},
                            )
            except Exception as e:
                log_and_capture_error(
                    e,
                    self.logger,
                    f"Fitbit SpO2: failed to fetch chunk {start_str}–{end_str}",
                    extra={"user_id": str(user_id), "chunk_start": start_str, "chunk_end": end_str},
                )

            # --- Breathing Rate ---
            try:
                br_response = self._make_api_request(
                    db,
                    user_id,
                    f"/1/user/-/br/date/{start_str}/{end_str}.json",
                )
                for item in (br_response or {}).get("br", []):
                    try:
                        br_value = item["value"]["breathingRate"]
                        date_str = item["dateTime"]
                        parsed = datetime.strptime(date_str, "%Y-%m-%d")
                        recorded_at = datetime(parsed.year, parsed.month, parsed.day, tzinfo=timezone.utc)

                        sample = TimeSeriesSampleCreate(
                            id=uuid4(),
                            user_id=user_id,
                            source=self.provider_name,
                            recorded_at=recorded_at,
                            value=Decimal(str(br_value)),
                            series_type=SeriesType.respiratory_rate,
                        )
                        timeseries_service.crud.create(db, sample)
                        count += 1
                    except Exception as e:
                        log_and_capture_error(
                            e,
                            self.logger,
                            f"Fitbit BR: failed to save item {item.get('dateTime', 'unknown')}",
                            extra={"user_id": str(user_id), "date": item.get("dateTime")},
                        )
            except Exception as e:
                log_and_capture_error(
                    e,
                    self.logger,
                    f"Fitbit BR: failed to fetch chunk {start_str}–{end_str}",
                    extra={"user_id": str(user_id), "chunk_start": start_str, "chunk_end": end_str},
                )

        return count

    def load_body_composition(
        self,
        db: DbSession,
        user_id: UUID,
        start_dt: datetime,
        end_dt: datetime,
    ) -> int:
        """Fetch weight, BMI, and body fat percentage and save as TimeSeriesSamples.

        Chunked into 30-day windows due to Fitbit API limit.
        Returns the number of samples saved.
        """
        count = 0

        for start_str, end_str in self._date_range_chunks(start_dt, end_dt):
            # --- Weight + BMI ---
            try:
                weight_response = self._make_api_request(
                    db,
                    user_id,
                    f"/1/user/-/body/log/weight/date/{start_str}/{end_str}.json",
                )
                for entry in (weight_response or {}).get("weight", []):
                    try:
                        recorded_at = datetime.strptime(entry["date"], "%Y-%m-%d").replace(tzinfo=timezone.utc)

                        weight_val = entry.get("weight")
                        if weight_val is not None:
                            timeseries_service.crud.create(
                                db,
                                TimeSeriesSampleCreate(
                                    id=uuid4(),
                                    user_id=user_id,
                                    source=self.provider_name,
                                    recorded_at=recorded_at,
                                    value=Decimal(str(weight_val)),
                                    series_type=SeriesType.weight,
                                ),
                            )
                            count += 1

                        bmi_val = entry.get("bmi")
                        if bmi_val is not None:
                            timeseries_service.crud.create(
                                db,
                                TimeSeriesSampleCreate(
                                    id=uuid4(),
                                    user_id=user_id,
                                    source=self.provider_name,
                                    recorded_at=recorded_at,
                                    value=Decimal(str(bmi_val)),
                                    series_type=SeriesType.body_mass_index,
                                ),
                            )
                            count += 1
                    except Exception as e:
                        log_and_capture_error(
                            e,
                            self.logger,
                            f"Fitbit weight: failed to save entry {entry.get('date', 'unknown')}",
                            extra={"user_id": str(user_id), "date": entry.get("date")},
                        )
            except Exception as e:
                log_and_capture_error(
                    e,
                    self.logger,
                    f"Fitbit weight: failed to fetch chunk {start_str}–{end_str}",
                    extra={"user_id": str(user_id), "chunk_start": start_str, "chunk_end": end_str},
                )

            # --- Body Fat ---
            try:
                fat_response = self._make_api_request(
                    db,
                    user_id,
                    f"/1/user/-/body/log/fat/date/{start_str}/{end_str}.json",
                )
                for entry in (fat_response or {}).get("fat", []):
                    try:
                        recorded_at = datetime.strptime(entry["date"], "%Y-%m-%d").replace(tzinfo=timezone.utc)
                        fat_val = entry.get("fat")
                        if fat_val is None:
                            continue
                        timeseries_service.crud.create(
                            db,
                            TimeSeriesSampleCreate(
                                id=uuid4(),
                                user_id=user_id,
                                source=self.provider_name,
                                recorded_at=recorded_at,
                                value=Decimal(str(fat_val)),
                                series_type=SeriesType.body_fat_percentage,
                            ),
                        )
                        count += 1
                    except Exception as e:
                        log_and_capture_error(
                            e,
                            self.logger,
                            f"Fitbit body fat: failed to save entry {entry.get('date', 'unknown')}",
                            extra={"user_id": str(user_id), "date": entry.get("date")},
                        )
            except Exception as e:
                log_and_capture_error(
                    e,
                    self.logger,
                    f"Fitbit body fat: failed to fetch chunk {start_str}–{end_str}",
                    extra={"user_id": str(user_id), "chunk_start": start_str, "chunk_end": end_str},
                )

        return count

    def load_fitness_metrics(
        self,
        db: DbSession,
        user_id: UUID,
        start_dt: datetime,
        end_dt: datetime,
    ) -> int:
        """Fetch VO2 max (Cardio Score) and save as TimeSeriesSamples.

        Chunked into 30-day windows due to Fitbit API limit.
        Returns the number of samples saved.
        """
        count = 0

        for start_str, end_str in self._date_range_chunks(start_dt, end_dt):
            try:
                response = self._make_api_request(
                    db,
                    user_id,
                    f"/1/user/-/cardioscore/date/{start_str}/{end_str}.json",
                )
                if not response:
                    continue

                for item in response.get("cardioScore", []):
                    try:
                        vo2_raw = item.get("value", {}).get("vo2Max")
                        if vo2_raw is None or vo2_raw in ("NA", ""):
                            continue

                        # Value may be a range like "42-46" or a plain integer string "45"
                        vo2_value = Decimal(str(vo2_raw).split("-")[0])

                        date_str: str = item["dateTime"]
                        parsed = datetime.strptime(date_str, "%Y-%m-%d")
                        recorded_at = datetime(parsed.year, parsed.month, parsed.day, tzinfo=timezone.utc)

                        timeseries_service.crud.create(
                            db,
                            TimeSeriesSampleCreate(
                                id=uuid4(),
                                user_id=user_id,
                                source=self.provider_name,
                                recorded_at=recorded_at,
                                value=vo2_value,
                                series_type=SeriesType.vo2_max,
                            ),
                        )
                        count += 1
                    except Exception as e:
                        log_and_capture_error(
                            e,
                            self.logger,
                            f"Fitbit VO2 max: failed to save item {item.get('dateTime', 'unknown')}",
                            extra={"user_id": str(user_id), "date": item.get("dateTime")},
                        )
            except Exception as e:
                log_and_capture_error(
                    e,
                    self.logger,
                    f"Fitbit VO2 max: failed to fetch chunk {start_str}–{end_str}",
                    extra={"user_id": str(user_id), "chunk_start": start_str, "chunk_end": end_str},
                )

        return count

    def load_and_save_all(
        self,
        db: DbSession,
        user_id: UUID,
        start_time: datetime | str | None = None,
        end_time: datetime | str | None = None,
        is_first_sync: bool = False,
    ) -> dict[str, int]:
        """Load and save all 247 data domains for the given user and time range.

        Args:
            db: Database session.
            user_id: User identifier.
            start_time: Start of time range (ISO 8601 string or datetime); defaults to 30 days ago (UTC).
            end_time: End of time range (ISO 8601 string or datetime); defaults to now (UTC).
            is_first_sync: Accepted for Celery task API compatibility; not currently used.

        String inputs are parsed as ISO 8601. Each domain is wrapped in its own try/except
        so a failure in one domain does not abort the others.
        """
        if isinstance(start_time, str):
            start_time = datetime.fromisoformat(start_time.replace("Z", "+00:00"))
        if isinstance(end_time, str):
            end_time = datetime.fromisoformat(end_time.replace("Z", "+00:00"))

        start_dt: datetime = start_time or datetime.now(timezone.utc) - timedelta(days=30)
        end_dt: datetime = end_time or datetime.now(timezone.utc)

        results: dict[str, int] = {
            "sleep_sessions_synced": 0,
            "daily_activity_samples_synced": 0,
            "hrv_samples_synced": 0,
            "blood_respiratory_samples_synced": 0,
            "body_composition_samples_synced": 0,
            "fitness_metric_samples_synced": 0,
        }

        try:
            results["sleep_sessions_synced"] = self.load_sleep(db, user_id, start_dt, end_dt)
        except Exception as e:
            log_and_capture_error(
                e,
                self.logger,
                "Fitbit 247 sync: sleep failed",
                extra={"user_id": str(user_id), "domain": "sleep"},
            )

        try:
            results["daily_activity_samples_synced"] = self.load_daily_activity(db, user_id, start_dt, end_dt)
        except Exception as e:
            log_and_capture_error(
                e,
                self.logger,
                "Fitbit 247 sync: daily_activity failed",
                extra={"user_id": str(user_id), "domain": "daily_activity"},
            )

        try:
            results["hrv_samples_synced"] = self.load_hrv(db, user_id, start_dt, end_dt)
        except Exception as e:
            log_and_capture_error(
                e,
                self.logger,
                "Fitbit 247 sync: hrv failed",
                extra={"user_id": str(user_id), "domain": "hrv"},
            )

        try:
            results["blood_respiratory_samples_synced"] = self.load_blood_respiratory(db, user_id, start_dt, end_dt)
        except Exception as e:
            log_and_capture_error(
                e,
                self.logger,
                "Fitbit 247 sync: blood_respiratory failed",
                extra={"user_id": str(user_id), "domain": "blood_respiratory"},
            )

        try:
            results["body_composition_samples_synced"] = self.load_body_composition(db, user_id, start_dt, end_dt)
        except Exception as e:
            log_and_capture_error(
                e,
                self.logger,
                "Fitbit 247 sync: body_composition failed",
                extra={"user_id": str(user_id), "domain": "body_composition"},
            )

        try:
            results["fitness_metric_samples_synced"] = self.load_fitness_metrics(db, user_id, start_dt, end_dt)
        except Exception as e:
            log_and_capture_error(
                e,
                self.logger,
                "Fitbit 247 sync: fitness_metrics failed",
                extra={"user_id": str(user_id), "domain": "fitness_metrics"},
            )

        return results
