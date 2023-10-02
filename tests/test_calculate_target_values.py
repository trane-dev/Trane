from datetime import datetime

import numpy as np
import pandas as pd
import pytest

from trane.core.utils import calculate_target_values, clean_date, cutoff_data, to_offset
from trane.metadata import SingleTableMetadata
from trane.utils.testing_utils import create_mock_data, create_mock_data_metadata


@pytest.fixture()
def data():
    df = create_mock_data(return_single_dataframe=True)
    return df


@pytest.fixture()
def cutoff_df():
    df = pd.DataFrame(
        {"A": [1, 2, 3, 4, 5]},
        index=pd.date_range("2022-01-01", periods=5, freq="D"),
    )
    df.index = pd.to_datetime(df.index)
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


def test_to_offset_int():
    assert to_offset(5) == 5


def test_to_offset_str():
    assert to_offset("2d") == pd.Timedelta(2, unit="D")


def test_to_offset_timedelta():
    assert to_offset(pd.Timedelta(3, unit="H")) == pd.Timedelta(3, unit="H")


def test_to_offset_offset():
    assert to_offset(
        pd.tseries.offsets.BusinessDay(2),
    ) == pd.tseries.offsets.BusinessDay(2)


def test_to_offset_invalid():
    with pytest.raises(AssertionError):
        to_offset(-1)
    with pytest.raises(AssertionError):
        to_offset("0d")
    with pytest.raises(AssertionError):
        to_offset(pd.Timedelta(-1, unit="H"))
    with pytest.raises(AssertionError):
        to_offset(pd.tseries.offsets.BusinessDay(-1))


def test_clean_date():
    assert clean_date("2019-01-01") == pd.Timestamp(
        datetime.strptime("2019-01-01", "%Y-%m-%d"),
    )
    timestamp = pd.Timestamp(datetime.strptime("2019-01-01", "%Y-%m-%d"))
    assert clean_date(timestamp) == timestamp


def test_cutoff_data_int(cutoff_df):
    threshold = 2
    df_out, cutoff_time_out = cutoff_data(cutoff_df, threshold=threshold)

    expected_df = pd.DataFrame(
        {"A": [3, 4, 5]},
        index=pd.date_range("2022-01-03", periods=3, freq="D"),
    )
    expected_df.index = pd.to_datetime(expected_df.index)
    expected_cutoff_time = pd.Timestamp("2022-01-03")
    pd.testing.assert_frame_equal(df_out, expected_df, check_freq=False)
    assert cutoff_time_out == expected_cutoff_time


def test_cutoff_data_str_offset(cutoff_df):
    threshold = "2D"
    df_out, cutoff_time_out = cutoff_data(cutoff_df, threshold=threshold)

    expected_df = pd.DataFrame(
        {"A": [3, 4, 5]},
        index=pd.date_range("2022-01-03", periods=3, freq="D"),
    )
    expected_cutoff_time = pd.Timestamp("2022-01-03")
    pd.testing.assert_frame_equal(df_out, expected_df, check_freq=False)
    assert cutoff_time_out == expected_cutoff_time


@pytest.mark.parametrize("threshold", ["2022-01-03", pd.Timestamp("2022-01-03")])
def test_cutoff_data_str_timestamp(cutoff_df, threshold):
    df_out, cutoff_time_out = cutoff_data(cutoff_df, threshold=threshold)
    expected_df = pd.DataFrame(
        {"A": [3, 4, 5]},
        index=pd.date_range("2022-01-03", periods=3, freq="D"),
    )
    expected_cutoff_time = pd.Timestamp("2022-01-03")
    pd.testing.assert_frame_equal(df_out, expected_df, check_freq=False)
    assert cutoff_time_out == expected_cutoff_time


def test_cutoff_invalid_threshold(cutoff_df):
    threshold = "invalid"
    with pytest.raises(ValueError):
        cutoff_data(cutoff_df, threshold)


def test_empty_data():
    df = pd.DataFrame(
        {"A": []},
        index=pd.DatetimeIndex([], dtype="datetime64[ns]", freq=None),
    )
    threshold = 2
    expected_df = pd.DataFrame(
        {"A": []},
        index=pd.DatetimeIndex([], dtype="datetime64[ns]", freq=None),
    )
    expected_cutoff_time = None
    df_out, cutoff_time_out = cutoff_data(df, threshold)
    pd.testing.assert_frame_equal(df_out, expected_df)
    assert cutoff_time_out == expected_cutoff_time
