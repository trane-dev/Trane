from math import e

import numpy as np
import pandas as pd
import pytest

from trane.ops.threshold_functions import entropy_of_list, sample_unique_values


def test_entropy_ints():
    labels = [1, 3, 5, 2, 3, 5, 3, 2, 1, 3, 4, 5]
    assert entropy_of_list(labels) == entropy_alternative(labels)


@pytest.mark.parametrize("dtype", [("category"), ("string"), ("object")])
def test_entropy_categorical(dtype):
    labels = pd.Series(
        ["red", "red", "blue", "blue", "blue", "green", "green"],
        dtype=dtype,
    )
    assert entropy_of_list(labels) == entropy_alternative(labels)


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