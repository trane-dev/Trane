import pandas as pd
import pytest

from trane.metadata import SingleTableMetadata
from trane.typing.utils import set_dataframe_dtypes


@pytest.fixture
def sample_dataframe():
    df = pd.DataFrame({
        "num_col": [1, 2, 3],
        "float_col": [1.1, 2.2, 3.3],
        "bool_col": [True, False, True],
        "date_col": ["2021-01-01", "2021-01-02", "2021-01-03"],
        "cat_col": ["a", "b", "a"],
        "str_col": ["12345", "22222", "33333"],
    })
    for col in df:
        df[col] = df[col].astype("string")
    return df


@pytest.fixture
def sample_single_metadata():
    ml_types = {
        "num_col": "Integer",
        "float_col": "Double",
        "bool_col": "Boolean",
        "date_col": "Datetime",
        "cat_col": "Categorical",
        "str_col": "PostalCode",
    }
    single_metadata = SingleTableMetadata(
        ml_types=ml_types,
        primary_key="num_col",
        time_index="date_col",
    )
    return single_metadata


def test_numeric_conversion(sample_dataframe, sample_single_metadata):
    converted_df = set_dataframe_dtypes(sample_dataframe, sample_single_metadata)
    assert converted_df["num_col"].dtype == "int64[pyarrow]"
    assert all(
        converted_df["num_col"]
        == pd.to_numeric(sample_dataframe["num_col"], downcast="integer"),
    )


def test_float_conversion(sample_dataframe, sample_single_metadata):
    converted_df = set_dataframe_dtypes(sample_dataframe, sample_single_metadata)
    assert converted_df["float_col"].dtype == "float64[pyarrow]"


def test_boolean_conversion(sample_dataframe, sample_single_metadata):
    converted_df = set_dataframe_dtypes(sample_dataframe, sample_single_metadata)
    assert converted_df["bool_col"].dtype == "bool[pyarrow]"


def test_datetime_conversion(sample_dataframe, sample_single_metadata):
    converted_df = set_dataframe_dtypes(sample_dataframe, sample_single_metadata)
    assert converted_df["date_col"].dtype == "datetime64[ns]"


def test_categorical_conversion(sample_dataframe, sample_single_metadata):
    converted_df = set_dataframe_dtypes(sample_dataframe, sample_single_metadata)
    assert converted_df["cat_col"].dtype == "category"


def test_empty_dataframe():
    df = pd.DataFrame()
    metadata = metadata = SingleTableMetadata(
        ml_types={},
    )
    converted_df = set_dataframe_dtypes(df, metadata)
    assert converted_df.empty
