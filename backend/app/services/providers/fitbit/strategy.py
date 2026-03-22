from app.services.providers.base_strategy import BaseProviderStrategy
from app.services.providers.fitbit.data_247 import FitbitData
from app.services.providers.fitbit.oauth import FitbitOAuth
from app.services.providers.fitbit.workouts import FitbitWorkouts


class FitbitStrategy(BaseProviderStrategy):
    """Fitbit provider implementation."""

    def __init__(self) -> None:
        """Initialise OAuth, workouts, and 247-data handlers for Fitbit."""
        super().__init__()
        self.oauth = FitbitOAuth(
            user_repo=self.user_repo,
            connection_repo=self.connection_repo,
            provider_name=self.name,
            api_base_url=self.api_base_url,
        )
        self.workouts = FitbitWorkouts(
            workout_repo=self.workout_repo,
            connection_repo=self.connection_repo,
            provider_name=self.name,
            api_base_url=self.api_base_url,
            oauth=self.oauth,
        )
        self.data_247 = FitbitData(  # type: ignore[assignment]
            provider_name=self.name,
            api_base_url=self.api_base_url,
            oauth=self.oauth,
        )

    @property
    def name(self) -> str:
        """Unique identifier for the provider (lowercase)."""
        return "fitbit"

    @property
    def api_base_url(self) -> str:
        """Base URL for the Fitbit Web API."""
        return "https://api.fitbit.com"
