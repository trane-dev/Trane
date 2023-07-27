from typing import Dict

import pandas as pd

from trane.typing.column_schema import ColumnSchema
from trane.typing.inference_functions import (
    boolean_func,
    categorical_func,
    datetime_func,
    double_func,
    integer_func,
    natural_language_func,
)
from trane.typing.logical_types import (
    Boolean,
    Categorical,
    Datetime,
    Double,
    Integer,
    NaturalLanguage,
    Unknown,
)


def _infer_series_schema(series: pd.Series) -> ColumnSchema:
    inference_functions = {
        boolean_func: ColumnSchema(logical_type=Boolean),
        categorical_func: ColumnSchema(
            logical_type=Categorical,
            semantic_tags={"category"},
        ),
        datetime_func: ColumnSchema(logical_type=Datetime),
        double_func: ColumnSchema(logical_type=Double, semantic_tags={"numeric"}),
        integer_func: ColumnSchema(logical_type=Integer, semantic_tags={"numeric"}),
        natural_language_func: ColumnSchema(logical_type=NaturalLanguage),
    }
    for infer_func, column_schema in inference_functions.items():
        if infer_func(series) is True:
            return column_schema
    return ColumnSchema(logical_type=Unknown)


def infer_table_meta(
    df: pd.DataFrame,
    entity_col=None,
    time_col=None,
) -> Dict[str, ColumnSchema]:
    table_meta = {}
    for col in df.columns:
        column_schema = _infer_series_schema(df[col])
        table_meta[col] = column_schema
    if entity_col:
        table_meta[entity_col].semantic_tags.add("primary_key")
    if time_col:
        table_meta[time_col].semantic_tags.add("time_index")
    return table_meta
