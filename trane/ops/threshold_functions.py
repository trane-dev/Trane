import random

import numpy as np
import pandas as pd
from scipy import stats


def get_k_most_frequent(series, k=3):
    # get the top k most frequent values
    if series.dtype in ["category", "object", "string"]:
        return series.value_counts()[:k].index.tolist()
    raise ValueError("Series must be categorical or object dtype")


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
        if pd.api.types.is_categorical_dtype(labels):
            labels = labels.cat.codes
        else:
            labels, _ = pd.factorize(labels)
    _, counts = np.unique(labels, return_counts=True)
    return stats.entropy(counts, base=base)
