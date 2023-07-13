from datetime import datetime

import pandas as pd
import pytest

from trane import CutoffStrategy


def pytest_addoption(parser):
    parser.addoption("--sample", action="store", default=None)


@pytest.fixture
def sample(request):
    return request.config.getoption("--sample")


@pytest.fixture()
def make_fake_df():
    data = {
        "id": [1, 2, 2, 3, 3, 3],
        "date": [
            datetime(2023, 1, 1),
            datetime(2023, 1, 3),
            datetime(2023, 1, 4),
            datetime(2023, 1, 3),
            datetime(2023, 1, 4),
            datetime(2023, 1, 5),
        ],
        "state": ["MA", "NY", "NY", "NJ", "NJ", "CT"],
        "amount": [10.0, 20.0, 30.0, 40.0, 50.0, 60.0],
    }
    df = pd.DataFrame(data)
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values(by=["date"])
    return df


@pytest.fixture()
def make_fake_meta():
    meta = {
        "id": ("Integer", {"numeric", "index"}),
        "date": ("Datetime", {}),
        "state": ("Categorical", {"category"}),
        "amount": ("Double", {"numeric"}),
    }
    return meta


@pytest.fixture()
def make_cutoff_strategy():
    entity_col = "id"
    window_size = "2d"
    minimum_data = "2023-01-01"
    maximum_data = "2023-01-05"
    cutoff_strategy = CutoffStrategy(
        entity_col=entity_col,
        window_size=window_size,
        minimum_data=minimum_data,
        maximum_data=maximum_data,
    )
    return cutoff_strategy
