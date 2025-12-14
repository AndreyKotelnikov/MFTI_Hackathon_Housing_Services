# Запуск: pytest tests/test_churn_validation.py --test-csv="data/clean_data.csv" -s

import pytest
import pandas as pd

CHURN_DAYS = 31
PERIOD_FROM = pd.Timestamp('2025-09-01')
PERIOD_UNTIL = pd.Timestamp('2025-10-31')

@pytest.fixture
def churn_days():
    return CHURN_DAYS

@pytest.fixture
def period_until():
    return PERIOD_UNTIL


def pytest_addoption(parser):
    parser.addoption(
        "--test-csv",
        action="store",
        default=None,
        help="Path to CSV file with test data",
    )

@pytest.fixture(scope="session")
def test_csv_path(request):
    path = request.config.getoption("--test-csv")
    if not path:
        pytest.fail("Нужно передать --test-csv=path/to/file.csv")
    return path


@pytest.fixture(scope="session")
def load_test_data():
    def _load(filepath: str) -> pd.DataFrame:
        df = pd.read_csv(filepath, parse_dates=["event_dt"])
        return df
    return _load


@pytest.fixture(scope="session")
def test_data(test_csv_path, load_test_data):
    print(f"Loading data from {test_csv_path}...")
    df = load_test_data(test_csv_path)
    print(f"Loaded {len(df)} rows")
    return df
