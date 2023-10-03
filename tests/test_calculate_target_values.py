from datetime import datetime

import pandas as pd
import pytest

from trane.core.utils import (
    clean_date,
    cutoff_data,
    determine_gap_size,
    determine_start_index,
    generate_data_slices,
    set_dataframe_index,
    to_offset,
)
from trane.metadata import SingleTableMetadata
from trane.utils.testing_utils import (
    create_mock_data,
    create_mock_data_metadata,
)


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


def test_set_dataframe_index():
    df = pd.DataFrame({"A": [1, 2, 3], "B": [4, 5, 6]})
    df = set_dataframe_index(df, "A")
    assert df.index.name == "A"

    df = set_dataframe_index(df, "B")
    assert df.index.name == "B"


def test_determine_start_index(cutoff_df):
    minimum_data = 1
    assert determine_start_index(cutoff_df, minimum_data) == 1

    # first data point in data is 2022-01-01, so no data points before that
    assert determine_start_index(cutoff_df, "2022-01-01") == 0
    # 1 data point before 2022-01-02
    assert determine_start_index(cutoff_df, "2022-01-02") == 1


def test_determine_gap_size():
    assert determine_gap_size("1d") == pd.Timedelta("1d")
    assert determine_gap_size(2) == 2


def test_integer_window_size():
    # Data: A B C D E F G H I J K L

    # Slice 1: | A B C | D E F G H I J K L
    # Slice 2: A B C | D E F | G H I J K L
    # Slice 3: A B C D E F | G H I | J K L
    # Slice 4: A B C D E F G H I | J K L |

    # Legend:
    # |...| - Data window of size 3
    # Space between |...| - Gap of 0

    df = pd.DataFrame(
        {
            "data": list("ABCDEFGHIJKL"),
        },
    )
    slices = list(generate_data_slices(df, gap=0, window_size=3))
    expected_slices = [
        ["A", "B", "C"],
        ["D", "E", "F"],
        ["G", "H", "I"],
        ["J", "K", "L"],
    ]
    for slice_data, expected_data in zip(slices, expected_slices):
        slice_vals = slice_data[0]["data"].tolist()
        assert (
            slice_vals == expected_data
        ), f"Expected {expected_data}, but got {slice_vals}"


def test_timedelta_window_size():
    # Timestamps:  t0   t1   t2   t3   t4   t5   t6   t7
    # Data:        A    B    C    D    E    F    G    H

    # Slice 1: | A B | C D E F G H  (t0 to t1)
    # Slice 2: A B | C D | E F G H  (t2 to t3)
    # Slice 3: A B C D | E F | G H  (t4 to t5)
    # Slice 4: A B C D E F | G H |  (t6 to t7)

    # Legend:
    # |...| - Data window of timedelta size
    # Space between |...| - Gap of 0 timedleta

    df = pd.DataFrame(
        {"data": list("ABCDEFGH")},
        index=pd.date_range("2022-01-01", periods=8, freq="D"),
    )

    slices = list(
        generate_data_slices(
            df,
            window_size=pd.Timedelta(days=2),
            gap=0,
        ),
    )

    # Expected slices when window_size is 2 days and gap is 1 day
    expected_slices = [
        ["A", "B"],
        ["C", "D"],
        ["E", "F"],
        ["G", "H"],
    ]

    for slice_data, expected_data in zip(slices, expected_slices):
        slice_vals = slice_data[0]["data"].tolist()
        assert (
            slice_vals == expected_data
        ), f"Expected {expected_data}, but got {slice_vals}"


def test_with_gap_larger_than_window_size():
    # Index:      0    1    2    3    4    5    6    7
    # Data:       A    B    C    D    E    F    G    H

    # Slice 1: | A B | C D E F G H  (t0 to t1)
    # Slice 2: A B C D | E F | G H  (t4 to t5)

    # Legend:
    # |...| - Data window of size 2
    # Space between |...| - Gap of 4 units
    # We add the gap of 4, which brings us to index 5.
    # The second data slice should thus be ['F', 'G'] if we keep the window size of 2.
    df = pd.DataFrame({"data": list("ABCDEFGH")})

    window_size = 2
    gap = 4

    slices = list(generate_data_slices(df, window_size=window_size, gap=gap))

    expected_slices = [["A", "B"], ["F", "G"]]

    for slice_data, expected_data in zip(slices, expected_slices):
        slice_vals = slice_data[0]["data"].tolist()
        assert (
            slice_vals == expected_data
        ), f"Expected {expected_data}, but got {slice_vals}"


def test_to_offset():
    assert to_offset(5) == 5
    assert to_offset("2d") == pd.Timedelta(2, unit="D")
    assert to_offset(pd.Timedelta(3, unit="H")) == pd.Timedelta(3, unit="H")
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


def sum_amount(dataslice):
    total = dataslice["amount"].sum()
    return total


# def test_create_target_values(data):
#     # ensure each row is included in the target values
#     # make sure each dataslice gets 1 row of data (and only 1 row)
#     window_size = pd.Timedelta(days=5)
#     data["transaction_id"] = 1
#     data.sort_values(by=["transaction_time"], inplace=True)
#     target_values = calculate_target_values(
#         df=data.copy(),
#         target_dataframe_index="transaction_id",
#         labeling_function=sum_amount,
#         time_index="transaction_time",
#         window_size=window_size,
#     )
#     assert target_values["transaction_id"].tolist() == [1, 1, 1]
#     assert target_values["cutoff_time"].tolist() == [
#         pd.Timestamp("2022-01-01 00:27:57"),
#         pd.Timestamp("2022-01-06 00:48:50"),
#         pd.Timestamp("2022-01-11 00:46:09"),
#     ]
#     assert np.allclose(
#         target_values["sum_amount"].tolist(),
#         [13837.71, 12990.74, 11213.85],
#     )
