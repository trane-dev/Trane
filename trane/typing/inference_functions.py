import sys
from typing import Any, Iterable, Union

import numpy as np
import pandas as pd
from dateutil.parser import ParserError
from importlib_resources import files
from pandas.api import types as pdtypes

MAX_INT = sys.maxsize
MIN_INT = -sys.maxsize - 1
Tokens = Iterable[str]

COMMON_WORDS_SET = set(
    word.strip().lower()
    for word in files("trane.typing").joinpath("1-1000.txt").read_text().split("\n")
    if len(word) > 0
)

NL_delimiters = r"[- \[\].,!\?;\n]"


def categorical_func(series):
    if pdtypes.is_categorical_dtype(series.dtype):
        return True

    if pdtypes.is_string_dtype(series.dtype) and not col_is_datetime(series):
        categorical_threshold = 0.2

        return _is_categorical_series(series, categorical_threshold)

    if pdtypes.is_float_dtype(series.dtype) or pdtypes.is_integer_dtype(series.dtype):
        numeric_categorical_threshold = None
        if numeric_categorical_threshold is not None:
            return _is_categorical_series(series, numeric_categorical_threshold)
        else:
            return False

    return False


def integer_func(series):
    if integer_nullable_func(series) and not series.isnull().any():
        if pdtypes.is_object_dtype(series.dtype):
            return True
        return all(series.mod(1).eq(0))
    return False


def integer_nullable_func(series):
    if pdtypes.is_integer_dtype(series.dtype):
        threshold = None
        if threshold is not None:
            return not _is_categorical_series(series, threshold)
        else:
            return True
    elif pdtypes.is_float_dtype(series.dtype):

        def _is_valid_int(value):
            return value >= MIN_INT and value <= MAX_INT and value.is_integer()

        if not series.isnull().any():
            return False
        series_no_null = series.dropna()
        return all([_is_valid_int(v) for v in series_no_null])
    elif pdtypes.is_object_dtype(series.dtype):
        series_no_null = series.dropna()
        try:
            return series_no_null.map(
                lambda x: (isinstance(x, str) and isinstance(int(x), int)),
            ).all()
        except ValueError:
            return False

    return False


def double_func(series):
    if pdtypes.is_float_dtype(series.dtype):
        threshold = None
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
    if boolean_nullable_func(series) and not series.isnull().any():
        return True
    return False


boolean_inference_strings = [
    ["yes", "no"],
    ["y", "n"],
    ["true", "false"],
    ["t", "f"],
]
boolean_inference_ints = []


def boolean_nullable_func(series):
    if pdtypes.is_bool_dtype(series.dtype) and not pdtypes.is_categorical_dtype(
        series.dtype,
    ):
        return True
    elif pdtypes.is_object_dtype(series.dtype):
        series_no_null = series.dropna()
        try:
            series_no_null_unq = set(series_no_null)
            if series_no_null_unq in [
                {False, True},
                {True},
                {False},
            ]:
                return True
            series_lower = set(str(s).lower() for s in set(series_no_null))
            if series_lower in [
                set(boolean_list) for boolean_list in boolean_inference_strings
            ]:
                return True
        except (
            TypeError
        ):  # Necessary to check for non-hashable values because of object dtype consideration
            return False
    elif pdtypes.is_integer_dtype(series.dtype) and len(
        boolean_inference_ints,
    ):
        series_unique = set(series)
        if series_unique == set(boolean_inference_ints):
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


def col_is_datetime(col, datetime_format=None):
    """Determine if a dataframe column contains datetime values or not. Returns True if column
    contains datetimes, False if not. Optionally specify the datetime format string for the column.
    Will not infer numeric data as datetime."""

    if pd.api.types.is_datetime64_any_dtype(col):
        return True

    col = col.dropna()
    if len(col) == 0:
        return False

    try:
        if pd.api.types.is_numeric_dtype(pd.Series(col.values.tolist())):
            return False
    except AttributeError:
        # given our current minimum dependencies (pandas 1.3.0 and pyarrow 4.0.1)
        # if we have a string dtype series, calling `col.values.tolist()` throws an error
        # AttributeError: 'StringArray' object has no attribute 'tolist'
        # this try/except block handles that, among other potential issues for experimental dtypes
        # until we find a more appropriate solution
        pass

    col = col.astype(str)

    try:
        pd.to_datetime(
            col,
            errors="raise",
            format=datetime_format,
            infer_datetime_format=True,
        )
        return True

    except (ParserError, ValueError, OverflowError, TypeError):
        return False


def _is_categorical_series(series: pd.Series, threshold: float) -> bool:
    """
    Return ``True`` if the given series is "likely" to be categorical.
    Otherwise, return ``False``.  We say that a series is "likely" to be
    categorical if the percentage of unique values relative to total non-NA
    values is below a certain threshold.  In other words, if all values in the
    series are accounted for by a sufficiently small collection of unique
    values, then the series is categorical.
    """
    try:
        nunique = series.nunique()
    except TypeError as e:
        # It doesn't seem like there's a more elegant way to do this.  Pandas
        # doesn't provide an API that would give you any indication ahead of
        # time if a series with object dtype has any unhashable elements.
        if "unhashable type" in e.args[0]:
            return False
        else:
            raise  # pragma: no cover
    if nunique == 0:
        return False

    pct_unique = nunique / series.count()
    return pct_unique <= threshold
