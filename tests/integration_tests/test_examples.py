import os

import pytest

import trane
from trane.datasets.load_functions import load_airbnb_reviews
from trane.typing import infer_table_meta
from trane.typing.column_schema import ColumnSchema
from trane.typing.logical_types import Categorical

from .utils import generate_and_verify_prediction_problem


@pytest.fixture
def current_dir():
    return os.path.dirname(__file__)


def test_airbnb_reviews(sample):
    df = load_airbnb_reviews()

    entity_col = "location"
    time_col = "date"
    window_size = "1m"

    meta = infer_table_meta(df, entity_col, time_col)
    meta["location"] = ColumnSchema(
        Categorical, semantic_tags={"category", "primary_key"}
    )

    cutoff_strategy = trane.CutoffStrategy(
        entity_col=entity_col,
        window_size=window_size,
    )
    generate_and_verify_prediction_problem(
        df=df,
        meta=meta,
        entity_col=entity_col,
        time_col=time_col,
        cutoff_strategy=cutoff_strategy,
        sample=sample,
        use_multiprocess=False,
    )


# Skipping test store as it took 3 hours for Github Actions to run
"""
def test_store(sample):
    dataframes, relationships = load_store()

    target_entity = "orderlines"
    entity_col = "orderid"
    time_col = "orderdate"
    window_size = "1m"

    df = denormalize(dataframes, relationships, target_entity)
    meta = infer_table_meta(df, entity_col, time_col)

    cutoff_strategy = trane.CutoffStrategy(
        entity_col=entity_col,
        window_size=window_size,
    )
    generate_and_verify_prediction_problem(
        df=df,
        meta=meta,
        entity_col=entity_col,
        time_col=time_col,
        cutoff_strategy=cutoff_strategy,
        sample=sample,
        use_multiprocess=False,
    )
"""
