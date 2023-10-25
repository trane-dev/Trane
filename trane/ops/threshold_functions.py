import random

import numpy as np
import pandas as pd
from pandas.api.types import (
    is_integer_dtype,
    is_object_dtype,
    is_string_dtype,
)
from scipy import stats

from trane.typing.ml_types import (
    convert_op_type,
)


def get_semantic_tags(filter_op):
    """
    Extract the semantic tags from the filter operation, looking at the input_output_types.

    Return:
        valid_semantic_tags(set(str)): a set of semantic tags that the filter operation can be applied to.
    """
    valid_semantic_tags = set()
    for op_input_type, _ in filter_op.input_output_types:
        op_input_type = convert_op_type(op_input_type)
        valid_semantic_tags.update(op_input_type.get_tags())
    return valid_semantic_tags


def recommend_categorical_thresholds(df, filter_op, k=3):
    thresholds = get_k_most_frequent(
        df[filter_op.column_name],
        k=k,
    )
    thresholds = list(set(thresholds))
    return thresholds


def entropy_of_series(series, base=None):
    if isinstance(series, pd.Series):
        # check if pandas categorical dtype
        if isinstance(series.dtype, pd.CategoricalDtype):
            series = series.cat.codes
        else:
            series, _ = pd.factorize(series)
    _, counts = np.unique(series, return_counts=True)
    return stats.entropy(counts, base=base)


def find_threshold_to_maximize_uncertainty(
    df,
    column_name,
    problem_type,
    filter_op,
    n_quantiles=10,
):
    """
    Find a threshold to split the data in the column_name of df to maximize uncertainty.

    Parameters:
    - df: DataFrame containing the data.
    - column_name: Name of the column for which the threshold should be found.
    - problem_type: Type of the problem (regression, classification).
    - n_quantiles: Number of quantiles to consider for thresholds.

    Returns:
    - Best threshold value to maximize uncertainty.
    """
    # Use quantiles as potential thresholds
    thresholds = df[column_name].quantile(np.linspace(0, 1, n_quantiles)).unique()
    max_uncertainty = 0  # Starting from 0 as initial value
    best_threshold = None
    original_threshold = filter_op.threshold

    for potential_threshold in thresholds:
        filter_op.set_parameters(threshold=potential_threshold)
        left_split = filter_op.label_function(df)
        right_split_indices = df.index.difference(left_split.index)
        right_split = df.loc[right_split_indices]

        # Compute uncertainty based on the task type
        if problem_type == "classification":
            left_uncertainty = entropy_of_series(left_split[column_name])
            right_uncertainty = entropy_of_series(right_split[column_name])
        elif problem_type == "regression":
            left_uncertainty = left_split[column_name].var()
            right_uncertainty = right_split[column_name].var()

        if pd.isna(left_uncertainty):
            left_uncertainty = 0
        if pd.isna(right_uncertainty):
            right_uncertainty = 0

        # Compute weighted average of uncertainties
        current_uncertainty = (
            len(left_split) * left_uncertainty + len(right_split) * right_uncertainty
        ) / len(df)

        if current_uncertainty > max_uncertainty:
            max_uncertainty = current_uncertainty
            best_threshold = potential_threshold

    filter_op.set_parameters(threshold=original_threshold)
    return best_threshold


def get_k_most_frequent(series, k=3):
    # get the top k most frequent values
    dtype = series.dtype
    if (
        is_object_dtype(dtype)
        or isinstance(dtype, pd.CategoricalDtype)
        or is_string_dtype(dtype)
        or is_integer_dtype(dtype)
    ):
        return series.value_counts()[:k].index.tolist()
    raise ValueError("Series must be categorical, string, object or int dtype")


def sample_unique_values(series, max_num_unique_values=10, random_state=None):
    sampled_unique_values = series.unique().tolist()
    if len(sampled_unique_values) > max_num_unique_values:
        random.seed(random_state)
        sampled_unique_values = random.sample(
            sampled_unique_values,
            max_num_unique_values,
        )
    return sampled_unique_values
