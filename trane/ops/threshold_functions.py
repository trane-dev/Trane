import random

import numpy as np
import pandas as pd
from pandas.api.types import (
    is_object_dtype,
    is_string_dtype,
)
from scipy import stats

from trane.typing.ml_types import (
    convert_op_type,
)


def _threshold_recommend(filter_op, df):
    yielded_thresholds = []
    valid_semantic_tags = get_semantic_tags(filter_op)

    if "category" in valid_semantic_tags:
        yielded_thresholds = recommend_categorical_thresholds(
            df,
            filter_op,
        )
    elif "numeric" in valid_semantic_tags:
        yielded_thresholds = recommend_numeric_thresholds(
            df=df,
            filter_op=filter_op,
        )
    return yielded_thresholds


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


def recommend_numeric_thresholds(
    df,
    filter_op,
    keep_rates=[0.25, 0.5, 0.75],
):
    thresholds = []
    for keep_rate in keep_rates:
        threshold = filter_op.find_threshold_by_fraction_of_data_to_keep(
            fraction_of_data_target=keep_rate,
            df=df,
            label_col=filter_op.column_name,
        )
        thresholds.append(threshold)
    return thresholds


def get_k_most_frequent(series, k=3):
    # get the top k most frequent values
    if (
        is_object_dtype(series.dtype)
        or isinstance(series.dtype, pd.CategoricalDtype)
        or is_string_dtype(series.dtype)
    ):
        return series.value_counts()[:k].index.tolist()
    raise ValueError("Series must be categorical, string or object dtype")


def sample_unique_values(series, max_num_unique_values=10, random_state=None):
    sampled_unique_values = series.unique().tolist()
    if len(sampled_unique_values) > max_num_unique_values:
        random.seed(random_state)
        sampled_unique_values = random.sample(
            sampled_unique_values,
            max_num_unique_values,
        )
    return sampled_unique_values


def entropy_of_list(labels, base=None):
    if isinstance(labels, pd.Series):
        # check if pandas categorical dtype
        if isinstance(labels.dtype, pd.CategoricalDtype):
            labels = labels.cat.codes
        else:
            labels, _ = pd.factorize(labels)
    _, counts = np.unique(labels, return_counts=True)
    return stats.entropy(counts, base=base)
