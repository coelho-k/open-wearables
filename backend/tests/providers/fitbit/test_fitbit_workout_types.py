from app.constants.workout_types.fitbit import get_unified_workout_type
from app.schemas.workout_types import WorkoutType


def test_running_maps_correctly() -> None:
    assert get_unified_workout_type(90009, "Run") == WorkoutType.RUNNING


def test_walking_maps_correctly() -> None:
    assert get_unified_workout_type(90013, "Walk") == WorkoutType.WALKING


def test_unknown_id_falls_back_to_name() -> None:
    assert get_unified_workout_type(99999, "Yoga") == WorkoutType.YOGA


def test_unknown_id_and_name_returns_other() -> None:
    assert get_unified_workout_type(99999, "Unknown Activity XYZ") == WorkoutType.OTHER
