from datetime import datetime

import pandas as pd
from woodwork.column_schema import ColumnSchema
from woodwork.logical_types import (
    Categorical,
    Datetime,
    Double,
)

import trane


def make_fake_dataset():
    data = {
        "id": [1, 2, 2, 3, 3],
        "date": [
            datetime(2023, 1, 1),
            datetime(2023, 1, 2),
            datetime(2023, 1, 2),
            datetime(2023, 1, 3),
            datetime(2023, 1, 4),
        ],
        "state": ["CA", "CA", "CA", "NY", "NY"],
        "country": ["US", "US", "US", "UK", "UK"],
        "amount": [10, 20, 30, 40, 50],
    }
    df = pd.DataFrame(data)
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values(by=["date"])
    return df


def make_fake_meta():
    meta = {
        "id": ColumnSchema(logical_type=Categorical, semantic_tags={"index"}),
        "date": ColumnSchema(logical_type=Datetime),
        "amount": ColumnSchema(logical_type=Double, semantic_tags={"numeric"}),
    }
    return meta


def test_prediction_problem():
    df = make_fake_dataset()
    entity_col = "id"
    time_col = "date"
    cutoff = "2d"
    cutoff_base = "2023-01-01"
    cutoff_end = "2023-01-04"
    cutoff_strategy = trane.CutoffStrategy(
        entity_col=entity_col,
        window_size=cutoff,
        minimum_data=cutoff_base,
        maximum_data=cutoff_end,
    )
    cutoff = cutoff_strategy.window_size

    meta = make_fake_meta()
    problem_generator = trane.PredictionProblemGenerator(
        table_meta=meta,
        entity_col=entity_col,
        cutoff_strategy=cutoff_strategy,
        time_col=time_col,
    )
    problems = problem_generator.generate(df, generate_thresholds=True)
    for p in problems:
        # print(p.operations)
        print(p)
