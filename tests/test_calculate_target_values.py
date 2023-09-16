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
    # Calculate the average time differences between each row
    data["transaction_time"].diff().mean()

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
    label_times = calculate_target_values_cp(
        df=data.copy(),
        target_dataframe_index="transaction_id",
        labeling_function=sum_amount,
        time_index="transaction_time",
        window_size=window_size,
    )
    for col in label_times:
        assert all(target_values[col] == label_times[col])


def calculate_target_values_cp(
    df,
    target_dataframe_index,
    labeling_function,
    time_index,
    window_size,
):
    import composeml as cp

    _label_maker = cp.LabelMaker(
        target_dataframe_index=target_dataframe_index,
        time_index=time_index,
        labeling_function=labeling_function,
        window_size=window_size,
    )
    label_times = _label_maker.search(
        df=df,
        num_examples_per_instance=-1,
        minimum_data=None,
        maximum_data=None,
        gap=None,
        drop_empty=True,
        verbose=False,
    )
    label_times = pd.DataFrame(
        label_times.values,
        index=label_times.index,
        columns=label_times.columns,
    )
    label_times = label_times.rename(columns={"time": "cutoff_time"})
    return label_times
