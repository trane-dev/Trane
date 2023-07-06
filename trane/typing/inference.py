import sys
from typing import Any, Iterable, Union

import numpy as np
import pandas as pd
import woodwork as ww
from importlib_resources import files
from pandas.api import types as pdtypes
from woodwork.type_sys.utils import _is_categorical_series, col_is_datetime


def _infer_series_type(series):
    inference_functions = {
        boolean_func: ("Boolean", "bool"),
        categorical_func: ("Categorical", "category"),
        datetime_func: ("Datetime", "datetime64[ns]"),
        double_func: ("Double", "float64"),
        integer_func: ("Integer", "int64"),
        natural_language_func: ("NaturalLanguage", "string"),
    }
    for infer_func, (ltype, dtype) in inference_functions.items():
        if infer_func(series) is True:
            return ltype, dtype
    return ("Unknown", "string")


def infer_types(df):
    if isinstance(df, pd.Series):
        _, dtype = _infer_series_type(df)
        df = df.astype(dtype)
        return df
    col_to_dtype = {}
    for col in df:
        _, dtype = _infer_series_type(df[col])
        df[col] = df[col].astype(dtype)
        col_to_dtype[col] = dtype
    return df


MAX_INT = sys.maxsize
MIN_INT = -sys.maxsize - 1
Tokens = Iterable[str]

COMMON_WORDS_SET = set(
    word.strip().lower()
    for word in files("trane.utils").joinpath("1-1000.txt").read_text().split("\n")
    if len(word) > 0
)

NL_delimiters = r"[- \[\].,!\?;\n]"


def categorical_func(series):
    if pdtypes.is_categorical_dtype(series.dtype):
        return True

    if pdtypes.is_string_dtype(series.dtype) and not col_is_datetime(series):
        categorical_threshold = ww.config.get_option("categorical_threshold")

        return _is_categorical_series(series, categorical_threshold)

    if pdtypes.is_float_dtype(series.dtype) or pdtypes.is_integer_dtype(series.dtype):
        numeric_categorical_threshold = ww.config.get_option(
            "numeric_categorical_threshold",
        )
        if numeric_categorical_threshold is not None:
            return _is_categorical_series(series, numeric_categorical_threshold)
        else:
            return False

    return False


def integer_func(series):
    if not series.isnull().any():
        if pdtypes.is_object_dtype(series.dtype):
            return True
        return all(series.mod(1).eq(0))
    return False


def double_func(series):
    if pdtypes.is_float_dtype(series.dtype):
        threshold = ww.config.get_option("numeric_categorical_threshold")
        if threshold is not None:
            return not _is_categorical_series(series, threshold)
        else:
            return True
    elif pdtypes.is_object_dtype(series.dtype):
        series_no_null = series.dropna()
        try:
            # If str and casting to float works, make sure that it isn't just an integer
            return series_no_null.map(
                lambda x: isinstance(x, str) and not float(x).is_integer(),
            ).any()
        except ValueError:
            return False

    return False


def boolean_func(series):
    if not series.isnull().any():
        return True
    return False


def datetime_func(series):
    if col_is_datetime(series):
        return True
    return False


def num_common_words(wordlist: Union[Tokens, Any]) -> float:
    if not isinstance(wordlist, Iterable):
        return np.nan
    num_common_words = 0
    for x in wordlist:
        if x.lower() in COMMON_WORDS_SET:
            num_common_words += 1
    return num_common_words


def natural_language_func(series):
    tokens = series.astype("string").str.split(NL_delimiters)
    mean_num_common_words = np.nanmean(tokens.map(num_common_words))

    return mean_num_common_words > 1.14
