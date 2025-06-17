import datetime
import sys

import numpy as np
import pandas as pd
import pytest

from trane.metadata.metadata import _str_to_ml_types
from trane.typing.inference import (
    _infer_ml_type,
    infer_ml_types,
)
from trane.typing.inference_functions import (
    pandas_modulo,
)
from trane.typing.ml_types import (
    Boolean,
    Datetime,
    Double,
    Integer,
    Unknown,
)


@pytest.fixture
def pandas_integers():
    return [
        pd.Series(4 * [-1, 2, 1, 7]),
        pd.Series(4 * [-1, 0, 5, 3]),
        pd.Series(4 * [sys.maxsize, -sys.maxsize - 1, 0]),
    ]


@pytest.fixture
def pandas_bools():
    return [
        pd.Series([True, False, True, True]),
        pd.Series(["y", "n", "N", "Y"]),
        pd.Series(["True", "false", "FALSE", "TRUE"]),
        pd.Series(["t", "f", "T", "T"]),
        pd.Series(["yes", "no", "NO", "Yes"]),
    ]


@pytest.fixture
def pandas_doubles():
    return [
        pd.Series(4 * [-1, 2.5, 1, 7]),
        pd.Series(4 * [1.5, np.nan, 1, 3]),
        pd.Series(4 * [1.5, np.inf, 1, 3]),
    ]


@pytest.fixture
def pandas_categories():
    return [
        pd.Series(10 * ["a", "b", "a", "b"]),
        pd.Series(10 * ["1", "2", "1", "2"]),
        pd.Series(10 * ["a", np.nan, "b", "b"]),
        pd.Series(10 * [1, 2, 1, 2]),
    ]


@pytest.fixture
def pandas_datetimes():
    return [
        pd.Series(["2000-3-11", "2000-3-12", "2000-03-13", "2000-03-14"]),
        pd.Series(["2000-3-11", np.nan, "2000-03-13", "2000-03-14"]),
    ]


def test_boolean_inference(pandas_bools):
    dtypes = ["bool", "boolean", "boolean[pyarrow]", "bool_", "object"]
    for series in pandas_bools:
        for dtype in dtypes:
            series = series.astype(dtype)
            ml_type = _infer_ml_type(series)
            assert isinstance(ml_type, Boolean)
            assert Boolean.inference_func(series) is True


def test_unknown_inference():
    series = pd.Series(["123.3", "123.2", "123.2"], dtype="string")
    ml_type = _infer_ml_type(series)
    assert isinstance(ml_type, Unknown)
    assert ml_type.dtype == "string[pyarrow]"


def test_double_inference(pandas_doubles):
    dtypes = ["float32", "float64", "float64[pyarrow]", "float32[pyarrow]"]
    for series in pandas_doubles:
        for dtype in dtypes:
            ml_type = _infer_ml_type(series.astype(dtype))
            assert isinstance(ml_type, Double)
            assert Double.inference_func(series.astype(dtype)) is True


def test_datetime_inference(pandas_datetimes):
    dtypes = ["object", "string", "datetime64[ns]"]
    for series in pandas_datetimes:
        for dtype in dtypes:
            ml_type = _infer_ml_type(series.astype(dtype))
            assert isinstance(ml_type, Datetime)
            assert Datetime.inference_func(series.astype(dtype)) is True


def test_integer_inference(pandas_integers):
    dtypes = [
        "int8",
        "int16",
        "int32",
        "int64",
        "int64[pyarrow]",
        "intp",
        "int",
    ]

    for series in pandas_integers:
        for dtype in dtypes:
            ml_type = _infer_ml_type(series.astype(dtype))
            assert isinstance(ml_type, Integer)
            assert Integer.inference_func(series.astype(dtype)) is True


def test_infer_ml_types():
    df = pd.DataFrame(
        {
            "a": [1, 2, 3],
            "b": [True, False, True],
            "c": ["a", "b", "c"],
            "d": [
                datetime.datetime(2019, 1, 1),
                datetime.datetime(2019, 1, 2),
                datetime.datetime(2019, 1, 3),
            ],
        },
    )
    metadata = infer_ml_types(df)
    for col, ml_type in metadata.items():
        assert col in df.columns
        assert str(ml_type).lower() in _str_to_ml_types()
    assert isinstance(metadata["a"], Integer)
    assert metadata["a"].get_tags() == {"numeric"}
    assert isinstance(metadata["b"], Boolean)
    assert isinstance(metadata["c"], Unknown)
    assert isinstance(metadata["d"], Datetime)
    assert metadata["d"].get_tags() == set()


def test_pandas_modulo():
    dtypes = ["int64", "int64[pyarrow]"]
    for dtype in dtypes:
        series = pd.Series([1, 2, 3, 4, 5], dtype=dtype)
        assert pandas_modulo(series, 1).tolist() == [0, 0, 0, 0, 0]
