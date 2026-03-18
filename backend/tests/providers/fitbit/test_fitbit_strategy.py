from app.services.providers.factory import ProviderFactory
from app.services.providers.fitbit.strategy import FitbitStrategy


def test_factory_returns_fitbit_strategy():
    factory = ProviderFactory()
    strategy = factory.get_provider("fitbit")
    assert isinstance(strategy, FitbitStrategy)


def test_fitbit_strategy_name():
    strategy = FitbitStrategy()
    assert strategy.name == "fitbit"


def test_fitbit_strategy_api_base_url():
    strategy = FitbitStrategy()
    assert strategy.api_base_url == "https://api.fitbit.com"


def test_fitbit_strategy_has_oauth():
    strategy = FitbitStrategy()
    assert strategy.oauth is not None


def test_fitbit_strategy_has_workouts():
    strategy = FitbitStrategy()
    assert strategy.workouts is not None


def test_fitbit_strategy_has_cloud_api():
    strategy = FitbitStrategy()
    assert strategy.has_cloud_api is True
