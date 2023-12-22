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
