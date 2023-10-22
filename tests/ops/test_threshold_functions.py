from math import e

import numpy as np
import pandas as pd
import pytest

from trane.ops import EqFilterOp, GreaterFilterOp
from trane.ops.threshold_functions import (
    entropy_of_series,
    find_threshold_to_maximize_uncertainty,
    get_k_most_frequent,
    sample_unique_values,
)


@pytest.mark.parametrize("dtype", [("category"), ("string"), ("object")])
def test_get_k_most_frequent(dtype):
    series = pd.Series(
        ["r", "r", "r", "r", "b", "b", "b", "g", "g", "t", "x"],
        dtype=dtype,
    )
    most_frequent = get_k_most_frequent(series, k=3)
    assert len(most_frequent) == 3
    assert most_frequent == ["r", "b", "g"]


@pytest.mark.parametrize(
    "dtype",
    [
        ("int64"),
        ("int64[pyarrow]"),
    ],
)
def test_get_k_most_frequent_raises(dtype):
    series = pd.Series([1, 2, 3, 4, 5], dtype=dtype)
    with pytest.raises(ValueError):
        get_k_most_frequent(series)


def test_entropy_ints():
    labels = [1, 3, 5, 2, 3, 5, 3, 2, 1, 3, 4, 5]
    assert np.isclose(entropy_of_series(labels), entropy_alternative(labels), atol=1e-3)


@pytest.mark.parametrize("dtype", [("category"), ("string"), ("object")])
def test_entropy_categorical(dtype):
    labels = pd.Series(
        ["red", "red", "blue", "blue", "blue", "green", "green"],
        dtype=dtype,
    )
    assert np.isclose(entropy_of_series(labels), entropy_alternative(labels), atol=1e-3)


def entropy_alternative(labels, base=None):
    vc = pd.Series(labels).value_counts(normalize=True, sort=False)
    base = e if base is None else base
    return -(vc * np.log(vc) / np.log(base)).sum()


def test_sample_unique_values():
    # Test with a series with less than max_num_unique_values unique values
    series = pd.Series([1, 2, 3, 4, 5])
    unique_values = sample_unique_values(series, max_num_unique_values=3)
    assert len(unique_values) == 3

    # Test with a series with exactly max_num_unique_values unique values
    series = pd.Series([1, 2, 3, 4, 5])
    unique_values = sample_unique_values(series, max_num_unique_values=5)
    assert len(unique_values) == 5

    # Test with a series with more than max_num_unique_values unique values
    series = pd.Series([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11])
    unique_values = sample_unique_values(series, max_num_unique_values=5)
    assert len(unique_values) == 5

    # Test with a series with only one unique value
    series = pd.Series([1, 1, 1, 1, 1])
    assert sample_unique_values(series) == [1]


def test_entropy_of_series_binary():
    series = pd.Series(["a", "a", "b", "b"])
    result = entropy_of_series(series, base=2)
    assert result == pytest.approx(1.0, rel=1e-6)


def test_entropy_of_series_multiclass():
    series = pd.Series(["a", "a", "b", "c"])
    result = entropy_of_series(series, base=2)
    assert result == pytest.approx(1.5, rel=1e-6)


def test_entropy_of_series_with_base():
    series = pd.Series(["a", "a", "b", "b"])
    result = entropy_of_series(series, base=10)
    assert result == pytest.approx(0.3010299956639812, rel=1e-6)


def test_classification_threshold():
    df = pd.DataFrame(
        {
            "price": [1, 2, 3, 4, 5],
            "B": ["a", "a", "b", "b", "c"],
        },
    )
    filter_op = EqFilterOp("price")
    result = find_threshold_to_maximize_uncertainty(
        df,
        "price",
        "classification",
        filter_op=filter_op,
    )
    assert np.isclose(result, 1.4444, atol=1e-4)


def test_regression_threshold():
    df = pd.DataFrame(
        {
            "price": [1, 2, 3, 4, 5],
            "Y": [1, 2, 2.5, 3.5, 5],
        },
    )
    filter_op = GreaterFilterOp("price")
    result = find_threshold_to_maximize_uncertainty(
        df,
        "price",
        "regression",
        filter_op=filter_op,
    )
    assert np.isclose(result, 5.0, atol=1e-6)


def test_classification_threshold_imbalanced():
    df = pd.DataFrame(
        {
            "price": [1, 2, 3, 4, 5],
            "B": ["a", "a", "a", "b", "c"],
        },
    )
    filter_op = EqFilterOp("price")
    result = find_threshold_to_maximize_uncertainty(
        df,
        "price",
        "classification",
        filter_op=filter_op,
    )
    assert np.isclose(result, 1.4444, atol=1e-4)


def test_regression_with_large_values():
    df = pd.DataFrame(
        {
            "price": [1000, 2000, 3000, 4000, 5000],
            "Y": [100, 200, 250, 350, 500],
        },
    )
    filter_op = GreaterFilterOp("price")
    result = find_threshold_to_maximize_uncertainty(
        df,
        "price",
        "regression",
        filter_op=filter_op,
    )
    assert np.isclose(result, 5000.0, atol=1e-6)


def test_regression():
    df = pd.DataFrame(
        {
            "id": [1, 2, 3],
            "purchase_date": ["2018-01-01", "2018-01-02", "2018-01-03"],
            "price": [10.50, 20.25, 30.01],
            "first_purchase": [True, False, False],
            "card_type": ["visa", "mastercard", "visa"],
        },
    )
    filter_op = GreaterFilterOp("price")
    result = find_threshold_to_maximize_uncertainty(
        df,
        "price",
        "regression",
        filter_op=filter_op,
    )
    assert np.isclose(result, 30.01, atol=1e-6)
