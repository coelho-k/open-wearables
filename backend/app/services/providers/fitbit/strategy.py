from app.services.providers.base_strategy import BaseProviderStrategy
from app.services.providers.fitbit.oauth import FitbitOAuth
from app.services.providers.fitbit.workouts import FitbitWorkouts


class FitbitStrategy(BaseProviderStrategy):
    """Fitbit provider implementation."""

    def __init__(self) -> None:
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

    @property
    def name(self) -> str:
        return "fitbit"

    @property
    def api_base_url(self) -> str:
        return "https://api.fitbit.com"
