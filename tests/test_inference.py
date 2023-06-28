import pandas as pd

from trane.utils.inference import (
    _infer_series_type,
)


def test_boolean_inference():
    series = pd.Series([True, False, True, False], dtype="string")
    l_type, dtype = _infer_series_type(series)
    assert l_type == "Boolean"
    assert dtype == "bool"
