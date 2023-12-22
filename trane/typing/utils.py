import pandas as pd
from pandas.api.types import (
    is_bool_dtype,
    is_datetime64_any_dtype,
    is_integer_dtype,
    is_numeric_dtype,
)


def set_dataframe_dtypes(dataframe, metadata):
    for col, ml_type in metadata.ml_types.items():
        actual_dtype = dataframe[col].dtype
        expected_dtype = ml_type.dtype
        if ml_type.is_numeric and not is_numeric_dtype(actual_dtype):
            if is_integer_dtype(expected_dtype):
                dataframe[col] = dataframe[col].astype(expected_dtype)
            else:
                dataframe[col] = dataframe[col].astype(expected_dtype)
        elif ml_type.is_datetime and not is_datetime64_any_dtype(actual_dtype):
            dataframe[col] = pd.to_datetime(dataframe[col])
        elif ml_type.is_categorical and not isinstance(
            actual_dtype,
            pd.CategoricalDtype,
        ):
            dataframe[col] = dataframe[col].astype(expected_dtype)
        elif ml_type.is_boolean and not is_bool_dtype(actual_dtype):
            dataframe[col] = dataframe[col].astype(expected_dtype)
        elif actual_dtype != expected_dtype:
            dataframe[col] = dataframe[col].astype(expected_dtype)
    return dataframe
