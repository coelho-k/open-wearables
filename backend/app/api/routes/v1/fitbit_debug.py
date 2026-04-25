"""Fitbit-specific debug endpoints. Returns raw Fitbit API responses so we can
see exactly what Fitbit serves vs what OW persists. Protected by API key."""

from typing import Any
from uuid import UUID

from fastapi import APIRouter

from app.database import DbSession
from app.services import ApiKeyDep
from app.services.providers.factory import ProviderFactory

router = APIRouter(prefix="/fitbit")
factory = ProviderFactory()


@router.get("/users/{user_id}/raw/activities/{date_str}")
def raw_activities_for_date(
    user_id: UUID,
    date_str: str,
    db: DbSession,
    _api_key: ApiKeyDep,
) -> dict[str, Any]:
    """Hit Fitbit's /1/user/-/activities/date/{date}.json and return the raw body."""
    strategy = factory.get_provider("fitbit")
    data_247 = getattr(strategy, "data_247", None)
    if data_247 is None:
        return {"error": "Fitbit data_247 strategy unavailable"}
    response = data_247._make_api_request(  # type: ignore[attr-defined]
        db, user_id, f"/1/user/-/activities/date/{date_str}.json"
    )
    return {"fitbit_endpoint": f"/1/user/-/activities/date/{date_str}.json", "response": response}


@router.get("/users/{user_id}/raw/profile")
def raw_profile(
    user_id: UUID,
    db: DbSession,
    _api_key: ApiKeyDep,
) -> dict[str, Any]:
    """Hit Fitbit's /1/user/-/profile.json and return the raw body (notably timezone)."""
    strategy = factory.get_provider("fitbit")
    data_247 = getattr(strategy, "data_247", None)
    if data_247 is None:
        return {"error": "Fitbit data_247 strategy unavailable"}
    response = data_247._make_api_request(db, user_id, "/1/user/-/profile.json")  # type: ignore[attr-defined]
    return {"fitbit_endpoint": "/1/user/-/profile.json", "response": response}


@router.get("/users/{user_id}/raw/path")
def raw_arbitrary_path(
    user_id: UUID,
    db: DbSession,
    _api_key: ApiKeyDep,
    fitbit_path: str,
) -> dict[str, Any]:
    """Hit an arbitrary Fitbit Web API path — diagnostic only.
    Example: ?fitbit_path=/1/user/-/activities/heart/date/2026-04-22/1d.json"""
    strategy = factory.get_provider("fitbit")
    data_247 = getattr(strategy, "data_247", None)
    if data_247 is None:
        return {"error": "Fitbit data_247 strategy unavailable"}
    response = data_247._make_api_request(db, user_id, fitbit_path)  # type: ignore[attr-defined]
    return {"fitbit_endpoint": fitbit_path, "response": response}


@router.get("/users/{user_id}/raw/steps/{date_str}")
def raw_steps_intraday(
    user_id: UUID,
    date_str: str,
    db: DbSession,
    _api_key: ApiKeyDep,
) -> dict[str, Any]:
    """Hit Fitbit's step-intraday endpoint for a date. Helpful when the daily
    summary returns 0 but intraday has real data — indicates Fitbit's daily
    aggregation hasn't finalized yet."""
    strategy = factory.get_provider("fitbit")
    data_247 = getattr(strategy, "data_247", None)
    if data_247 is None:
        return {"error": "Fitbit data_247 strategy unavailable"}
    response = data_247._make_api_request(  # type: ignore[attr-defined]
        db, user_id, f"/1/user/-/activities/steps/date/{date_str}/1d.json"
    )
    return {"fitbit_endpoint": f"/1/user/-/activities/steps/date/{date_str}/1d.json", "response": response}
