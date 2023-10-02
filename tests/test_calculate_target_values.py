import numpy as np
import pandas as pd
import pytest

from trane.core.utils import calculate_target_values
from trane.metadata import SingleTableMetadata
from trane.utils.testing_utils import create_mock_data, create_mock_data_metadata


@pytest.fixture()
def data():
    df = create_mock_data(return_single_dataframe=True)
    return df


def test_create_mock_data_metadata(data):
    metadata = create_mock_data_metadata(single_table=True)
    assert isinstance(metadata, SingleTableMetadata)
    for col, ml_type in metadata.ml_types.items():
        assert col in data.columns
        if ml_type.dtype in ["float64[pyarrow]", "double[pyarrow]"]:
            assert str(data[col].dtype) == "double[pyarrow]"
        else:
            assert ml_type.dtype == str(data[col].dtype)
    for col in data.columns:
        assert col in metadata.ml_types
    assert len(metadata.ml_types) == len(data.columns)
    assert data["transaction_id"].is_unique


def sum_amount(dataslice):
    total = dataslice["amount"].sum()
    return total


def test_create_target_values(data):
    # ensure each row is included in the target values
    # make sure each dataslice gets 1 row of data (and only 1 row)
    window_size = pd.Timedelta(days=5)
    data["transaction_id"] = 1
    data.sort_values(by=["transaction_time"], inplace=True)
    target_values = calculate_target_values(
        df=data.copy(),
        target_dataframe_index="transaction_id",
        labeling_function=sum_amount,
        time_index="transaction_time",
        window_size=window_size,
    )
    assert target_values["transaction_id"].tolist() == [1, 1, 1]
    assert target_values["cutoff_time"].tolist() == [
        pd.Timestamp("2022-01-01 00:27:57"),
        pd.Timestamp("2022-01-06 00:48:50"),
        pd.Timestamp("2022-01-11 00:46:09"),
    ]
    assert np.allclose(
        target_values["sum_amount"].tolist(),
        [13837.71, 12990.74, 11213.85],
    )
