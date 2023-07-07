import pandas as pd

from trane.typing.column_schema import ColumnSchema
from trane.typing.inference import (
    _infer_series_type,
)
from trane.typing.logical_types import (
    Boolean,
)


def test_boolean_inference():
    series = pd.Series([True, False, True, False], dtype="string")
    column_schema = _infer_series_type(series)
    assert isinstance(column_schema, ColumnSchema)
    assert column_schema.logical_type == Boolean
