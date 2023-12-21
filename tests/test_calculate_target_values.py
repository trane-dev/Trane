import pandas as pd
import pytest

from trane.core.utils import (
    generate_data_slices,
    set_dataframe_index,
)
from trane.metadata import SingleTableMetadata
from trane.utils import create_mock_data, create_mock_data_metadata


def test_set_dataframe_index():
    df = pd.DataFrame({"A": [1, 2, 3], "B": [4, 5, 6]})
    df = set_dataframe_index(df, "A")
    assert df.index.name == "A"
    df = set_dataframe_index(df, "B")
    assert df.index.name == "B"


@pytest.mark.parametrize(
    "window_size, gap, expected_dataslices",
    [
        # not implemented
        # ("2d", 0, [["A", "B"], ["C", "D"], ["E", "F"], ["G", "H"]]),
        # ("2d", "1d", [["A", "B"], ["B", "C"], ["C", "D"], ["D", "F"], ["F", "G"], ["G", "H"]]),
        # ("1d", "2d", [["A", "B"], ["B", "C"], ["C", "D"], ["D", "F"], ["F", "G"], ["G", "H"]]),
        ("1d", "1d", [["A"], ["B"], ["C"], ["D"], ["E"], ["F"], ["G"], ["H"]]),
        ("2d", "2d", [["A", "B"], ["C", "D"], ["E", "F"], ["G", "H"]]),
        ("3d", "3d", [["A", "B", "C"], ["D", "E", "F"], ["G", "H"]]),
        ("3d", "3d", [["A", "B", "C"], ["D", "E", "F"], ["G", "H"]]),
    ],
)
def test_generate_data_slices(window_size, gap, expected_dataslices):
    # Timestamps:  t0   t1   t2   t3   t4   t5   t6   t7
    # Data:        A    B    C    D    E    F    G    H

    df = pd.DataFrame(
        {
            "data": list("ABCDEFGH"),
            "timestamp": pd.date_range(start="2022-01-01", end="2022-01-08", freq="D"),
        },
    )
    df = set_dataframe_index(df, "timestamp")
    # start_times = ["2022-01-01", "2022-01-03", "2022-01-05", "2022-01-07"]
    # end_times = ["2022-01-02", "2022-01-04", "2022-01-06", "2022-01-08"]
    for dataslice, metadata in generate_data_slices(
        df=df,
        window_size=window_size,
        gap=gap,
    ):
        assert dataslice["data"].tolist() == expected_dataslices.pop(0)
        # TODO: Verify start times
    assert len(expected_dataslices) == 0


def test_create_mock_data_metadata():
    data = create_mock_data(return_single_dataframe=True)
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


# def test_integer_window_size():
#     # Data: A B C D E F G H I J K L

#     # Slice 1: | A B C | D E F G H I J K L
#     # Slice 2: A B C | D E F | G H I J K L
#     # Slice 3: A B C D E F | G H I | J K L
#     # Slice 4: A B C D E F G H I | J K L |
#     # |...| - Data window of size 3
#     # Space between |...| - Gap of 0
#     # A gap of 0 means that the windows are contiguous, i.e., there is no spacing between the end of one window and the start of the next.

#     df = pd.DataFrame(
#         {
#             "data": list("ABCDEFGHIJKL"),
#         },
#     )
#     slices = list(generate_data_slices(df, gap=0, window_size=3))
#     expected_slices = [
#         ["A", "B", "C"],
#         ["D", "E", "F"],
#         ["G", "H", "I"],
#         ["J", "K", "L"],
#     ]
#     check_data_slices(slices, expected_slices)
#     # Slice 1: | A B C | D E F G H I J K L
#     # Slice 2: A B C D | E F G | H I J K L
#     # Slice 3: A B C D E F G H | I J K | L
#     # |...| - Data window of size 3
#     # Space between |...| - Gap of 1
#     # A gap of 1 means that you skip one element in the data sequence when starting a new slice.
#     slices = list(generate_data_slices(df, gap=1, window_size=3))
#     expected_slices = [
#         ["A", "B", "C"],
#         ["E", "F", "G"],
#         ["I", "J", "K"],
#     ]
#     check_data_slices(slices, expected_slices)


# def test_gap_timedelta_window_size_timedelta():
#     # Timestamps:  t0   t1   t2   t3   t4   t5   t6   t7
#     # Data:        A    B    C    D    E    F    G    H
#     # Slice 1: | A | B C D E F G H  (t0 to t0)
#     # Slice 2: A B | C | D E F G H  (t3 to t3)
#     # Slice 3: A B C D | E | F G H  (t4 to t4)
#     # Slice 4: A B C D E F | G | H  (t5 to t5)

#     df = pd.DataFrame(
#         {"value": list("ABCDEFGH")},
#         index=pd.date_range(start="2022-01-01", end="2022-01-08", freq="D"),
#     )
#     slices = list(
#         generate_data_slices(
#             df,
#             window_size=pd.Timedelta(days=1),
#             gap=pd.Timedelta(days=1),
#         ),
#     )
#     expected_values = [
#         ["A"],
#         ["C"],
#         ["E"],
#         ["G"],
#     ]
#     for i, (dataslice, meta) in enumerate(slices):
#         assert dataslice["value"].tolist() == expected_values[i]


# def test_with_gap_larger_than_window_size():
#     # Index:      0    1    2    3    4    5    6    7
#     # Data:       A    B    C    D    E    F    G    H

#     # Slice 1: | A B | C D E F G H  (t0 to t1)
#     # Slice 2: A B C D | E F | G H  (t4 to t5)

#     # Legend:
#     # |...| - Data window of size 2
#     # Space between |...| - Gap of 4 units
#     # We add the gap of 4, which brings us to index 5.
#     # The second data slice should thus be ['F', 'G'] if we keep the window size of 2.
#     df = pd.DataFrame({"data": list("ABCDEFGH")})
#     slices = list(generate_data_slices(df, window_size=2, gap=3))
#     expected_slices = [["A", "B"], ["F", "G"]]
#     check_data_slices(slices, expected_slices)


# def check_data_slices(slices, expected_slices):
#     for slice_data, expected_data in zip(slices, expected_slices):
#         slice_vals = slice_data[0]["data"].tolist()
#         assert (
#             slice_vals == expected_data
#         ), f"Expected {expected_data}, but got {slice_vals}"


# def sum_amount(dataslice):
#     total = dataslice["amount"].sum()
#     return total


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
