from datetime import datetime

import pandas as pd
import pytest


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
        "is_fraud": [False, False, False, True, False, False],
    }
    df = pd.DataFrame(data)
    df = df.astype(
        {
            "id": "int64[pyarrow]",
            "date": "datetime64[ns]",
            "state": "category",
            "amount": "float64[pyarrow]",
            "is_fraud": "boolean[pyarrow]",
        },
    )
    df = df.sort_values(by=["date"])
    return df


@pytest.fixture()
def make_fake_meta():
    meta = {
        "id": ("Integer", {"numeric", "primary_key"}),
        "date": ("Datetime", {}),
        "state": ("Categorical", {"category"}),
        "amount": ("Double", {"numeric"}),
        "is_fraud": ("Boolean", {}),
    }
    return meta
