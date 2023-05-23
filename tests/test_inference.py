import pandas as pd

from trane.utils.inference import (
    infer_types,
)


def test_boolean_inference():
    series = pd.Series([True, False, True, False], dtype="string")
    assert infer_types(series) == "string"
