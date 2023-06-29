from datetime import datetime

import composeml as cp
import pandas as pd
import pytest
from woodwork.column_schema import ColumnSchema
from woodwork.logical_types import (
    Categorical,
    Datetime,
    Double,
)

import trane


@pytest.fixture()
def make_fake_df():
    data = {
        "id": [1, 2, 2, 3, 3, 3],
        "date": [
            datetime(2023, 1, 1),
            datetime(2023, 1, 3),
            datetime(2023, 1, 4),
            datetime(2023, 1, 3),
            datetime(2023, 1, 4),
            datetime(2023, 1, 5),
        ],
        "state": ["MA", "NY", "NY", "NJ", "NJ", "CT"],
        "amount": [10, 20, 30, 40, 50, 60],
    }
    df = pd.DataFrame(data)
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values(by=["date"])
    return df


@pytest.fixture()
def make_fake_meta():
    meta = {
        "id": ColumnSchema(logical_type=Categorical, semantic_tags={"index"}),
        "date": ColumnSchema(logical_type=Datetime),
        "country": ColumnSchema(logical_type=Categorical, semantic_tags={"category"}),
        "amount": ColumnSchema(logical_type=Double, semantic_tags={"numeric"}),
    }
    return meta


def num_observations(ds, **kwargs):
    return len(ds)


def column_gt_len(data_slice, column, value):
    return len(data_slice[data_slice[column] > value])


def test_prediction_problem(make_fake_df, make_fake_meta):
    df = make_fake_df
    meta = make_fake_meta
    entity_col = "id"
    time_col = "date"
    window_size = "2d"
    minimum_data = "2023-01-01"
    maximum_data = "2023-01-05"
    cutoff_strategy = trane.CutoffStrategy(
        entity_col=entity_col,
        window_size=window_size,
        minimum_data=minimum_data,
        maximum_data=maximum_data,
    )
    problem_generator = trane.PredictionProblemGenerator(
        table_meta=meta,
        entity_col=entity_col,
        cutoff_strategy=cutoff_strategy,
        time_col=time_col,
    )

    problems = problem_generator.generate(df, generate_thresholds=True)
    # bad integration testing
    # not ideal but okay to test for now
    for p in problems:
        label_times = p.execute(df, -1)
        label_times.rename(columns={"_execute_operations_on_df": "label"}, inplace=True)
        if str(p) == "For each <id> predict the number of records in next 2d days":
            assert label_times["label"].tolist() == [1, 2, 2, 1]

            expected_label_times = generate_label_times(
                num_observations,
                df,
                minimum_data,
                maximum_data,
                window_size,
            )
            pd.testing.assert_frame_equal(
                expected_label_times,
                label_times,
                check_frame_type=False,
            )
        elif (
            "For each <id> predict the number of records with <amount> greater than"
            in str(p)
        ):
            threshold = p.operations[0].hyper_parameter_settings["threshold"]
            if threshold == 40:
                assert label_times["label"].tolist() == [0, 0, 1, 1]
            elif threshold == 30:
                assert label_times["label"].tolist() == [0, 0, 2, 1]
            elif threshold == 10:
                assert label_times["label"].tolist() == [0, 2, 2, 1]
            expected_label_times = generate_label_times(
                column_gt_len,
                df,
                minimum_data,
                maximum_data,
                window_size,
                column="amount",
                value=threshold,
            )
            pd.testing.assert_frame_equal(
                expected_label_times,
                label_times,
                check_frame_type=False,
            )
        # elif str(p) == "For each <id> predict the number of records with <amount> greater than 20 in next 2d days":
        #     assert label_times["label"].tolist() == [1, 1, 1]
        #     print(label_times)


def generate_label_times(
    labeling_function,
    df,
    minimum_data,
    maximum_data,
    window_size,
    column=None,
    value=None,
    label_column_name="label",
):
    lm = cp.LabelMaker(
        target_dataframe_index="id",
        labeling_function=labeling_function,
        time_index="date",
        window_size=window_size,
    )
    lt = lm.search(
        df=df,
        column=column,
        value=value,
        num_examples_per_instance=-1,
        minimum_data=minimum_data,
        maximum_data=maximum_data,
        verbose=False,
    )
    # rename the third column to label
    lt.rename(columns={lt.columns[2]: label_column_name}, inplace=True)
    return lt
