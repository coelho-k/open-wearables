# backend/tests/providers/fitbit/test_fitbit_data_247.py
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock, patch
from uuid import UUID, uuid4
from zoneinfo import ZoneInfo

import pytest

from app.schemas.series_types import SeriesType
from app.services.providers.fitbit.data_247 import FitbitData


@pytest.fixture
def fitbit_data() -> FitbitData:
    oauth = MagicMock()
    return FitbitData(
        provider_name="fitbit",
        api_base_url="https://api.fitbit.com",
        oauth=oauth,
    )


def test_fitbit_data_has_provider_name(fitbit_data: FitbitData) -> None:
    assert fitbit_data.provider_name == "fitbit"


def test_fitbit_data_has_api_base_url(fitbit_data: FitbitData) -> None:
    assert fitbit_data.api_base_url == "https://api.fitbit.com"


def test_load_and_save_all_returns_expected_keys(fitbit_data: FitbitData) -> None:
    db = MagicMock()
    user_id = uuid4()

    with (
        patch.object(fitbit_data, "load_sleep", return_value=0),
        patch.object(fitbit_data, "load_daily_activity", return_value=0),
        patch.object(fitbit_data, "load_hrv", return_value=0),
        patch.object(fitbit_data, "load_blood_respiratory", return_value=0),
        patch.object(fitbit_data, "load_body_composition", return_value=0),
        patch.object(fitbit_data, "load_fitness_metrics", return_value=0),
    ):
        result = fitbit_data.load_and_save_all(db, user_id)

    assert set(result.keys()) == {
        "sleep_sessions_synced",
        "daily_activity_samples_synced",
        "hrv_samples_synced",
        "blood_respiratory_samples_synced",
        "body_composition_samples_synced",
        "fitness_metric_samples_synced",
    }


def test_load_and_save_all_parses_string_start_time(fitbit_data: FitbitData) -> None:
    db = MagicMock()
    user_id = uuid4()

    with (
        patch.object(fitbit_data, "load_sleep", return_value=1) as mock_sleep,
        patch.object(fitbit_data, "load_daily_activity", return_value=0),
        patch.object(fitbit_data, "load_hrv", return_value=0),
        patch.object(fitbit_data, "load_blood_respiratory", return_value=0),
        patch.object(fitbit_data, "load_body_composition", return_value=0),
        patch.object(fitbit_data, "load_fitness_metrics", return_value=0),
    ):
        result = fitbit_data.load_and_save_all(db, user_id, start_time="2026-01-01T00:00:00Z")

    assert result["sleep_sessions_synced"] == 1
    # load_sleep was called — datetime arg should be a datetime, not a string
    call_args = mock_sleep.call_args[0]
    assert isinstance(call_args[2], datetime)


def test_load_and_save_all_domain_error_does_not_abort_others(fitbit_data: FitbitData) -> None:
    db = MagicMock()
    user_id = uuid4()

    with (
        patch.object(fitbit_data, "load_sleep", side_effect=Exception("boom")),
        patch.object(fitbit_data, "load_daily_activity", return_value=5),
        patch.object(fitbit_data, "load_hrv", return_value=0),
        patch.object(fitbit_data, "load_blood_respiratory", return_value=0),
        patch.object(fitbit_data, "load_body_composition", return_value=0),
        patch.object(fitbit_data, "load_fitness_metrics", return_value=0),
    ):
        result = fitbit_data.load_and_save_all(db, user_id)

    assert result["sleep_sessions_synced"] == 0
    assert result["daily_activity_samples_synced"] == 5


# --- Sleep normalization tests ---

RAW_SLEEP = {
    "logId": 987654321,
    # Fitbit returns local time strings without timezone offset.
    "startTime": "2026-03-01T22:30:00.000",
    "endTime": "2026-03-02T06:30:00.000",
    "duration": 28800000,  # 8 hours in ms
    "efficiency": 88,
    "isMainSleep": True,
    "levels": {
        "summary": {
            "deep": {"minutes": 90},
            "light": {"minutes": 200},
            "rem": {"minutes": 80},
            "wake": {"minutes": 30},
        }
    },
}


def test_load_sleep_saves_session(fitbit_data: FitbitData) -> None:
    db = MagicMock()
    user_id = uuid4()

    api_response = {
        "sleep": [RAW_SLEEP],
        "pagination": {"next": ""},
    }

    with (
        patch.object(fitbit_data, "_make_api_request", return_value=api_response),
        patch("app.services.providers.fitbit.data_247.event_record_service") as mock_svc,
    ):
        mock_svc.create.return_value = MagicMock(id=uuid4())
        count = fitbit_data.load_sleep(
            db, user_id, datetime(2026, 3, 1, tzinfo=timezone.utc), datetime(2026, 3, 3, tzinfo=timezone.utc)
        )

    assert count == 1
    mock_svc.create.assert_called_once()
    mock_svc.create_detail.assert_called_once()


def test_load_sleep_external_id_is_string(fitbit_data: FitbitData) -> None:
    db = MagicMock()
    user_id = uuid4()

    api_response = {"sleep": [RAW_SLEEP], "pagination": {"next": ""}}

    with (
        patch.object(fitbit_data, "_make_api_request", return_value=api_response),
        patch("app.services.providers.fitbit.data_247.event_record_service") as mock_svc,
    ):
        mock_svc.create.return_value = MagicMock(id=uuid4())
        fitbit_data.load_sleep(
            db, user_id, datetime(2026, 3, 1, tzinfo=timezone.utc), datetime(2026, 3, 3, tzinfo=timezone.utc)
        )

    record_arg = mock_svc.create.call_args[0][1]
    assert record_arg.external_id == "987654321"
    assert isinstance(record_arg.external_id, str)


def test_load_sleep_nap_detection(fitbit_data: FitbitData) -> None:
    db = MagicMock()
    user_id = uuid4()
    nap = {**RAW_SLEEP, "isMainSleep": False}

    api_response = {"sleep": [nap], "pagination": {"next": ""}}

    with (
        patch.object(fitbit_data, "_make_api_request", return_value=api_response),
        patch("app.services.providers.fitbit.data_247.event_record_service") as mock_svc,
    ):
        mock_svc.create.return_value = MagicMock(id=uuid4())
        fitbit_data.load_sleep(
            db, user_id, datetime(2026, 3, 1, tzinfo=timezone.utc), datetime(2026, 3, 3, tzinfo=timezone.utc)
        )

    detail_arg = mock_svc.create_detail.call_args[0][1]
    assert detail_arg.is_nap is True


def test_load_sleep_time_in_bed_uses_duration_seconds(fitbit_data: FitbitData) -> None:
    db = MagicMock()
    user_id = uuid4()

    api_response = {"sleep": [RAW_SLEEP], "pagination": {"next": ""}}

    with (
        patch.object(fitbit_data, "_make_api_request", return_value=api_response),
        patch("app.services.providers.fitbit.data_247.event_record_service") as mock_svc,
    ):
        mock_svc.create.return_value = MagicMock(id=uuid4())
        fitbit_data.load_sleep(
            db, user_id, datetime(2026, 3, 1, tzinfo=timezone.utc), datetime(2026, 3, 3, tzinfo=timezone.utc)
        )

    detail_arg = mock_svc.create_detail.call_args[0][1]
    # 28800000 ms → 28800 s → 480 min
    assert detail_arg.sleep_time_in_bed_minutes == 480


def test_load_sleep_converts_local_time_to_utc(fitbit_data: FitbitData) -> None:
    """Fitbit returns naive local-time strings; they must be converted to UTC using user's TZ."""
    db = MagicMock()
    user_id = uuid4()
    # 10:30 PM in UTC-7 (America/Denver) = 05:30 UTC next day
    sleep_local = {**RAW_SLEEP, "startTime": "2026-03-01T22:30:00.000", "endTime": "2026-03-02T06:00:00.000"}
    api_response = {"sleep": [sleep_local], "pagination": {"next": ""}}

    with (
        patch.object(fitbit_data, "_get_user_timezone", return_value=ZoneInfo("America/Denver")),
        patch.object(fitbit_data, "_make_api_request", return_value=api_response),
        patch("app.services.providers.fitbit.data_247.event_record_service") as mock_svc,
    ):
        mock_svc.create.return_value = MagicMock(id=uuid4())
        fitbit_data.load_sleep(
            db, user_id, datetime(2026, 3, 1, tzinfo=timezone.utc), datetime(2026, 3, 3, tzinfo=timezone.utc)
        )

    record_arg = mock_svc.create.call_args[0][1]
    # 22:30 MDT (UTC-7) → 05:30 UTC next day
    assert record_arg.start_datetime == datetime(2026, 3, 2, 5, 30, tzinfo=timezone.utc)
    assert record_arg.end_datetime == datetime(2026, 3, 2, 13, 0, tzinfo=timezone.utc)


def test_load_sleep_empty_response(fitbit_data: FitbitData) -> None:
    db = MagicMock()
    user_id = uuid4()

    with patch.object(fitbit_data, "_make_api_request", return_value={}):
        count = fitbit_data.load_sleep(
            db, user_id, datetime(2026, 3, 1, tzinfo=timezone.utc), datetime(2026, 3, 3, tzinfo=timezone.utc)
        )

    assert count == 0


def test_load_sleep_item_error_continues(fitbit_data: FitbitData) -> None:
    db = MagicMock()
    user_id = uuid4()
    good_sleep = {**RAW_SLEEP, "logId": 111}
    bad_sleep = {**RAW_SLEEP, "logId": 222, "startTime": None}  # will cause fromisoformat error

    api_response = {"sleep": [bad_sleep, good_sleep], "pagination": {"next": ""}}

    with (
        patch.object(fitbit_data, "_make_api_request", return_value=api_response),
        patch("app.services.providers.fitbit.data_247.event_record_service") as mock_svc,
    ):
        mock_svc.create.return_value = MagicMock(id=uuid4())
        count = fitbit_data.load_sleep(
            db, user_id, datetime(2026, 3, 1, tzinfo=timezone.utc), datetime(2026, 3, 3, tzinfo=timezone.utc)
        )

    # Only good_sleep should succeed
    assert count == 1


# --- Daily activity tests ---

RAW_ACTIVITY_SUMMARY = {
    "summary": {
        "steps": 9800,
        "caloriesOut": 2400,
        "caloriesBMR": 1800,
        "floors": 12,
        "fairlyActiveMinutes": 30,
        "veryActiveMinutes": 20,
        "restingHeartRate": 62,
        # distances lives inside summary in the real Fitbit response
        "distances": [
            {"activity": "total", "distance": 7.5},
            {"activity": "tracker", "distance": 7.5},
        ],
    },
}


def test_load_daily_activity_saves_samples(fitbit_data: FitbitData) -> None:
    db = MagicMock()
    user_id = uuid4()

    with (
        patch.object(fitbit_data, "_make_api_request", return_value=RAW_ACTIVITY_SUMMARY),
        patch("app.services.providers.fitbit.data_247.timeseries_service") as mock_ts,
    ):
        count = fitbit_data.load_daily_activity(
            db,
            user_id,
            datetime(2026, 3, 1, tzinfo=timezone.utc),
            datetime(2026, 3, 1, tzinfo=timezone.utc),
        )

    # steps, energy, basal_energy, floors, distance, exercise_time, resting_hr = 7 samples
    assert count == 7
    assert mock_ts.crud.create.call_count == 7


def test_load_daily_activity_skips_absent_floors(fitbit_data: FitbitData) -> None:
    db = MagicMock()
    user_id = uuid4()
    summary_no_floors = {
        "summary": {k: v for k, v in RAW_ACTIVITY_SUMMARY["summary"].items() if k != "floors"},
    }

    with (
        patch.object(fitbit_data, "_make_api_request", return_value=summary_no_floors),
        patch("app.services.providers.fitbit.data_247.timeseries_service"),
    ):
        count = fitbit_data.load_daily_activity(
            db,
            user_id,
            datetime(2026, 3, 1, tzinfo=timezone.utc),
            datetime(2026, 3, 1, tzinfo=timezone.utc),
        )

    assert count == 6  # no floors


def test_load_daily_activity_skips_absent_resting_hr(fitbit_data: FitbitData) -> None:
    db = MagicMock()
    user_id = uuid4()
    summary_no_hr = {
        "summary": {k: v for k, v in RAW_ACTIVITY_SUMMARY["summary"].items() if k != "restingHeartRate"},
    }

    with (
        patch.object(fitbit_data, "_make_api_request", return_value=summary_no_hr),
        patch("app.services.providers.fitbit.data_247.timeseries_service"),
    ):
        count = fitbit_data.load_daily_activity(
            db,
            user_id,
            datetime(2026, 3, 1, tzinfo=timezone.utc),
            datetime(2026, 3, 1, tzinfo=timezone.utc),
        )

    assert count == 6  # no resting HR


def test_load_daily_activity_missing_summary_saves_nothing(fitbit_data: FitbitData) -> None:
    db = MagicMock()
    user_id = uuid4()
    # Response is truthy but has no "summary" key
    response_no_summary = {"activities": [], "goals": {}}

    with (
        patch.object(fitbit_data, "_make_api_request", return_value=response_no_summary),
        patch("app.services.providers.fitbit.data_247.timeseries_service") as mock_ts,
    ):
        count = fitbit_data.load_daily_activity(
            db,
            user_id,
            datetime(2026, 3, 1, tzinfo=timezone.utc),
            datetime(2026, 3, 1, tzinfo=timezone.utc),
        )

    assert count == 0
    mock_ts.crud.create.assert_not_called()


def test_load_daily_activity_distance_converted_to_meters(fitbit_data: FitbitData) -> None:
    db = MagicMock()
    user_id = uuid4()

    with (
        patch.object(fitbit_data, "_make_api_request", return_value=RAW_ACTIVITY_SUMMARY),
        patch("app.services.providers.fitbit.data_247.timeseries_service") as mock_ts,
    ):
        fitbit_data.load_daily_activity(
            db,
            user_id,
            datetime(2026, 3, 1, tzinfo=timezone.utc),
            datetime(2026, 3, 1, tzinfo=timezone.utc),
        )

    all_samples = [call[0][1] for call in mock_ts.crud.create.call_args_list]
    dist_samples = [s for s in all_samples if s.series_type == SeriesType.distance_walking_running]
    assert len(dist_samples) == 1
    assert dist_samples[0].value == Decimal("7500")


def test_load_daily_activity_recorded_at_is_midnight_utc(fitbit_data: FitbitData) -> None:
    db = MagicMock()
    user_id = uuid4()

    with (
        patch.object(fitbit_data, "_make_api_request", return_value=RAW_ACTIVITY_SUMMARY),
        patch("app.services.providers.fitbit.data_247.timeseries_service") as mock_ts,
    ):
        fitbit_data.load_daily_activity(
            db,
            user_id,
            datetime(2026, 3, 5, tzinfo=timezone.utc),
            datetime(2026, 3, 5, tzinfo=timezone.utc),
        )

    all_samples = [call[0][1] for call in mock_ts.crud.create.call_args_list]
    for sample in all_samples:
        assert sample.recorded_at == datetime(2026, 3, 5, tzinfo=timezone.utc)


# --- HRV tests ---

RAW_HRV = {
    "hrv": [
        {"dateTime": "2026-03-01", "value": {"dailyRmssd": 42.5, "deepRmssd": 38.1}},
        {"dateTime": "2026-03-02", "value": {"dailyRmssd": 45.0, "deepRmssd": 40.2}},
    ]
}


def test_load_hrv_saves_rmssd_samples(fitbit_data: FitbitData) -> None:
    db = MagicMock()
    user_id = uuid4()

    with (
        patch.object(fitbit_data, "_make_api_request", return_value=RAW_HRV),
        patch("app.services.providers.fitbit.data_247.timeseries_service") as mock_ts,
    ):
        count = fitbit_data.load_hrv(
            db,
            user_id,
            datetime(2026, 3, 1, tzinfo=timezone.utc),
            datetime(2026, 3, 2, tzinfo=timezone.utc),
        )

    assert count == 2
    assert mock_ts.crud.create.call_count == 2


def test_load_hrv_recorded_at_midnight_utc(fitbit_data: FitbitData) -> None:
    db = MagicMock()
    user_id = uuid4()

    with (
        patch.object(fitbit_data, "_make_api_request", return_value=RAW_HRV),
        patch("app.services.providers.fitbit.data_247.timeseries_service") as mock_ts,
    ):
        fitbit_data.load_hrv(
            db,
            user_id,
            datetime(2026, 3, 1, tzinfo=timezone.utc),
            datetime(2026, 3, 2, tzinfo=timezone.utc),
        )

    samples = [call[0][1] for call in mock_ts.crud.create.call_args_list]
    assert samples[0].recorded_at == datetime(2026, 3, 1, tzinfo=timezone.utc)
    assert samples[1].recorded_at == datetime(2026, 3, 2, tzinfo=timezone.utc)


def test_load_hrv_uses_daily_rmssd(fitbit_data: FitbitData) -> None:
    db = MagicMock()
    user_id = uuid4()
    single_entry_hrv = {"hrv": [RAW_HRV["hrv"][0]]}  # only the first entry

    with (
        patch.object(fitbit_data, "_make_api_request", return_value=single_entry_hrv),
        patch("app.services.providers.fitbit.data_247.timeseries_service") as mock_ts,
    ):
        fitbit_data.load_hrv(
            db,
            user_id,
            datetime(2026, 3, 1, tzinfo=timezone.utc),
            datetime(2026, 3, 1, tzinfo=timezone.utc),
        )

    sample = mock_ts.crud.create.call_args[0][1]
    assert sample.value == Decimal("42.5")
    assert sample.series_type == SeriesType.heart_rate_variability_rmssd


def test_load_hrv_chunks_over_30_days(fitbit_data: FitbitData) -> None:
    db = MagicMock()
    user_id = uuid4()

    with patch.object(fitbit_data, "_make_api_request", return_value={"hrv": []}) as mock_req:
        fitbit_data.load_hrv(
            db,
            user_id,
            datetime(2026, 1, 1, tzinfo=timezone.utc),
            datetime(2026, 3, 15, tzinfo=timezone.utc),  # 73 days → 3 chunks
        )

    # 73 days: chunk1 = Jan1-Jan30, chunk2 = Jan31-Mar1, chunk3 = Mar2-Mar15
    assert mock_req.call_count == 3


# --- Blood & respiratory tests ---

RAW_SPO2 = [
    {"dateTime": "2026-03-01", "value": {"avg": 97.5, "min": 95.0, "max": 99.0}},
    {"dateTime": "2026-03-02", "value": {"avg": 98.0, "min": 96.0, "max": 99.5}},
]

RAW_BR = {
    "br": [
        {"dateTime": "2026-03-01", "value": {"breathingRate": 14.2}},
        {"dateTime": "2026-03-02", "value": {"breathingRate": 15.0}},
    ]
}


def test_load_blood_respiratory_saves_spo2_and_br(fitbit_data: FitbitData) -> None:
    db = MagicMock()
    user_id = uuid4()

    def api_side_effect(_db: Any, _uid: UUID, endpoint: str, params: Any = None) -> Any:
        if "spo2" in endpoint:
            return RAW_SPO2
        return RAW_BR

    with (
        patch.object(fitbit_data, "_make_api_request", side_effect=api_side_effect),
        patch("app.services.providers.fitbit.data_247.timeseries_service") as mock_ts,
    ):
        count = fitbit_data.load_blood_respiratory(
            db,
            user_id,
            datetime(2026, 3, 1, tzinfo=timezone.utc),
            datetime(2026, 3, 2, tzinfo=timezone.utc),
        )

    # 2 SpO2 + 2 BR = 4
    assert count == 4
    assert mock_ts.crud.create.call_count == 4


def test_load_blood_respiratory_spo2_not_list_returns_zero(fitbit_data: FitbitData) -> None:
    db = MagicMock()
    user_id = uuid4()
    single_br = {"br": [RAW_BR["br"][0]]}  # one BR entry only

    def api_side_effect(_db: Any, _uid: UUID, endpoint: str, params: Any = None) -> Any:
        if "spo2" in endpoint:
            return {"error": "no data"}  # not a list — guard should skip it
        return single_br

    with (
        patch.object(fitbit_data, "_make_api_request", side_effect=api_side_effect),
        patch("app.services.providers.fitbit.data_247.timeseries_service"),
    ):
        count = fitbit_data.load_blood_respiratory(
            db,
            user_id,
            datetime(2026, 3, 1, tzinfo=timezone.utc),
            datetime(2026, 3, 1, tzinfo=timezone.utc),
        )

    # SpO2 skipped (not a list), only BR saved (1 item)
    assert count == 1


def test_load_blood_respiratory_spo2_series_type(fitbit_data: FitbitData) -> None:
    db = MagicMock()
    user_id = uuid4()

    def api_side_effect(_db: Any, _uid: UUID, endpoint: str, params: Any = None) -> Any:
        if "spo2" in endpoint:
            return [RAW_SPO2[0]]
        return {"br": []}

    with (
        patch.object(fitbit_data, "_make_api_request", side_effect=api_side_effect),
        patch("app.services.providers.fitbit.data_247.timeseries_service") as mock_ts,
    ):
        fitbit_data.load_blood_respiratory(
            db,
            user_id,
            datetime(2026, 3, 1, tzinfo=timezone.utc),
            datetime(2026, 3, 1, tzinfo=timezone.utc),
        )

    sample = mock_ts.crud.create.call_args[0][1]
    assert sample.series_type == SeriesType.oxygen_saturation
    assert sample.value == Decimal("97.5")


# --- Body composition tests ---

RAW_WEIGHT = {
    "weight": [
        {"date": "2026-03-01", "time": "07:30:00", "weight": 75.5, "bmi": 23.1},
        {"date": "2026-03-02", "time": "07:35:00", "weight": 75.3, "bmi": 23.0},
    ]
}

RAW_FAT = {
    "fat": [
        {"date": "2026-03-01", "time": "07:30:00", "fat": 18.5},
    ]
}


def test_load_body_composition_saves_weight_bmi_fat(fitbit_data: FitbitData) -> None:
    db = MagicMock()
    user_id = uuid4()

    def api_side_effect(_db: Any, _uid: UUID, endpoint: str, params: Any = None) -> Any:
        if "weight" in endpoint:
            return RAW_WEIGHT
        return RAW_FAT

    with (
        patch.object(fitbit_data, "_make_api_request", side_effect=api_side_effect),
        patch("app.services.providers.fitbit.data_247.timeseries_service") as mock_ts,
    ):
        count = fitbit_data.load_body_composition(
            db,
            user_id,
            datetime(2026, 3, 1, tzinfo=timezone.utc),
            datetime(2026, 3, 2, tzinfo=timezone.utc),
        )

    # 2 weight + 2 BMI + 1 fat = 5
    assert count == 5
    assert mock_ts.crud.create.call_count == 5


def test_load_body_composition_weight_series_type(fitbit_data: FitbitData) -> None:
    db = MagicMock()
    user_id = uuid4()

    def api_side_effect(_db: Any, _uid: UUID, endpoint: str, params: Any = None) -> Any:
        if "weight" in endpoint:
            return {"weight": [RAW_WEIGHT["weight"][0]]}
        return {"fat": []}

    with (
        patch.object(fitbit_data, "_make_api_request", side_effect=api_side_effect),
        patch("app.services.providers.fitbit.data_247.timeseries_service") as mock_ts,
    ):
        fitbit_data.load_body_composition(
            db,
            user_id,
            datetime(2026, 3, 1, tzinfo=timezone.utc),
            datetime(2026, 3, 1, tzinfo=timezone.utc),
        )

    all_samples = [call[0][1] for call in mock_ts.crud.create.call_args_list]
    weight_samples = [s for s in all_samples if s.series_type == SeriesType.weight]
    bmi_samples = [s for s in all_samples if s.series_type == SeriesType.body_mass_index]
    assert len(weight_samples) == 1
    assert len(bmi_samples) == 1
    assert weight_samples[0].value == Decimal("75.5")
    assert bmi_samples[0].value == Decimal("23.1")


def test_load_body_composition_fat_series_type(fitbit_data: FitbitData) -> None:
    db = MagicMock()
    user_id = uuid4()

    def api_side_effect(_db: Any, _uid: UUID, endpoint: str, params: Any = None) -> Any:
        if "weight" in endpoint:
            return {"weight": []}
        return {"fat": [RAW_FAT["fat"][0]]}

    with (
        patch.object(fitbit_data, "_make_api_request", side_effect=api_side_effect),
        patch("app.services.providers.fitbit.data_247.timeseries_service") as mock_ts,
    ):
        fitbit_data.load_body_composition(
            db,
            user_id,
            datetime(2026, 3, 1, tzinfo=timezone.utc),
            datetime(2026, 3, 1, tzinfo=timezone.utc),
        )

    all_samples = [call[0][1] for call in mock_ts.crud.create.call_args_list]
    fat_samples = [s for s in all_samples if s.series_type == SeriesType.body_fat_percentage]
    assert len(fat_samples) == 1
    assert fat_samples[0].value == Decimal("18.5")


def test_load_body_composition_recorded_at_includes_time(fitbit_data: FitbitData) -> None:
    """recorded_at must include the time component so multiple entries on the same day are not deduplicated."""
    db = MagicMock()
    user_id = uuid4()

    def api_side_effect(_db: Any, _uid: UUID, endpoint: str, params: Any = None) -> Any:
        if "weight" in endpoint:
            return {"weight": [RAW_WEIGHT["weight"][0]]}  # date=2026-03-01, time=07:30:00
        return {"fat": [RAW_FAT["fat"][0]]}  # date=2026-03-01, time=07:30:00

    with (
        patch.object(fitbit_data, "_make_api_request", side_effect=api_side_effect),
        patch("app.services.providers.fitbit.data_247.timeseries_service") as mock_ts,
    ):
        fitbit_data.load_body_composition(
            db,
            user_id,
            datetime(2026, 3, 1, tzinfo=timezone.utc),
            datetime(2026, 3, 1, tzinfo=timezone.utc),
        )

    all_samples = [call[0][1] for call in mock_ts.crud.create.call_args_list]
    for sample in all_samples:
        # Must NOT be midnight — should preserve the 07:30:00 time from the Fitbit response
        assert sample.recorded_at != datetime(2026, 3, 1, tzinfo=timezone.utc), (
            "recorded_at must include the time component, not be truncated to midnight"
        )
        assert sample.recorded_at == datetime(2026, 3, 1, 7, 30, 0, tzinfo=timezone.utc)


# --- Fitness metrics (VO2 max) tests ---

RAW_CARDIO = {
    "cardioScore": [
        {"dateTime": "2026-03-01", "value": {"vo2Max": "45"}},
        {"dateTime": "2026-03-02", "value": {"vo2Max": "42-46"}},  # range value
        {"dateTime": "2026-03-03", "value": {"vo2Max": "NA"}},  # skip
        {"dateTime": "2026-03-04", "value": {"vo2Max": None}},  # skip
    ]
}


def test_load_fitness_metrics_saves_vo2_max(fitbit_data: FitbitData) -> None:
    db = MagicMock()
    user_id = uuid4()

    with (
        patch.object(fitbit_data, "_make_api_request", return_value=RAW_CARDIO),
        patch("app.services.providers.fitbit.data_247.timeseries_service") as mock_ts,
    ):
        count = fitbit_data.load_fitness_metrics(
            db,
            user_id,
            datetime(2026, 3, 1, tzinfo=timezone.utc),
            datetime(2026, 3, 4, tzinfo=timezone.utc),
        )

    # Only 2 valid: "45" and "42-46" (NA and None skipped)
    assert count == 2
    assert mock_ts.crud.create.call_count == 2


def test_load_fitness_metrics_range_value_uses_lower_bound(fitbit_data: FitbitData) -> None:
    db = MagicMock()
    user_id = uuid4()

    range_response = {"cardioScore": [{"dateTime": "2026-03-01", "value": {"vo2Max": "42-46"}}]}

    with (
        patch.object(fitbit_data, "_make_api_request", return_value=range_response),
        patch("app.services.providers.fitbit.data_247.timeseries_service") as mock_ts,
    ):
        fitbit_data.load_fitness_metrics(
            db,
            user_id,
            datetime(2026, 3, 1, tzinfo=timezone.utc),
            datetime(2026, 3, 1, tzinfo=timezone.utc),
        )

    sample = mock_ts.crud.create.call_args[0][1]
    assert sample.value == Decimal("42")
    assert sample.series_type == SeriesType.vo2_max


def test_load_fitness_metrics_skips_na(fitbit_data: FitbitData) -> None:
    db = MagicMock()
    user_id = uuid4()

    na_response = {"cardioScore": [{"dateTime": "2026-03-01", "value": {"vo2Max": "NA"}}]}

    with (
        patch.object(fitbit_data, "_make_api_request", return_value=na_response),
        patch("app.services.providers.fitbit.data_247.timeseries_service") as mock_ts,
    ):
        count = fitbit_data.load_fitness_metrics(
            db,
            user_id,
            datetime(2026, 3, 1, tzinfo=timezone.utc),
            datetime(2026, 3, 1, tzinfo=timezone.utc),
        )

    assert count == 0
    mock_ts.crud.create.assert_not_called()
