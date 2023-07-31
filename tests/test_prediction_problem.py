import composeml as cp
import pandas as pd

import trane


def num_observations(data_slice, **kwargs):
    return len(data_slice)


def column_gt_len(data_slice, column, value, **kwargs):
    return len(data_slice[data_slice[column] > value])


def column_lt_len(data_slice, column, value, **kwargs):
    return len(data_slice[data_slice[column] < value])


def column_gt_op(data_slice, column, value, operation="sum", **kwargs):
    calculated = data_slice[data_slice[column] > value]
    if calculated.empty:
        return pd.NA
    else:
        if operation is None:
            return calculated[column].iloc[0]
        return getattr(calculated[column], operation)()


def column_lt_op(data_slice, column, value, operation="sum", **kwargs):
    calculated = data_slice[data_slice[column] < value]
    if calculated.empty:
        return pd.NA
    else:
        if operation is None:
            return calculated[column].iloc[0]
        return getattr(calculated[column], operation)()


def sum_column(data_slice, column, **kwargs):
    return data_slice[column].sum()


def avg_column(data_slice, column, **kwargs):
    return data_slice[column].mean()


def max_column(data_slice, column, **kwargs):
    return data_slice[column].max()


def min_column(data_slice, column, **kwargs):
    return data_slice[column].min()


def test_prediction_problem_with_no_entity(
    make_fake_df,
    make_fake_meta,
    make_cutoff_strategy,
):
    entity_col = None
    time_col = "date"
    df = make_fake_df
    meta = make_fake_meta
    for column in df.columns:
        assert column in meta
    cutoff_strategy = make_cutoff_strategy

    problem_generator = trane.PredictionProblemGenerator(
        df=df,
        table_meta=meta,
        entity_col=entity_col,
        cutoff_strategy=cutoff_strategy,
        time_col=time_col,
    )
    problems = problem_generator.generate(df, generate_thresholds=True)
    for problem in problems:
        assert problem.entity_col is None
        assert problem.time_col == time_col
        assert problem.__str__().startswith("Predict")


def test_prediction_problem(make_fake_df, make_fake_meta, make_cutoff_strategy):
    entity_col = "id"
    time_col = "date"
    df = make_fake_df
    meta = make_fake_meta
    for column in df.columns:
        assert column in meta
    cutoff_strategy = make_cutoff_strategy

    problem_generator = trane.PredictionProblemGenerator(
        df=df,
        table_meta=meta,
        entity_col=entity_col,
        cutoff_strategy=cutoff_strategy,
        time_col=time_col,
    )

    problems = problem_generator.generate(df, generate_thresholds=True)
    verify_problems(problems, df, cutoff_strategy)


def verify_problems(problems, df, cutoff_strategy):
    problems_verified = 0
    # bad integration testing
    # not ideal but okay to test for now
    problems = sorted(problems)
    for p in problems:
        label_times = p.execute(df, -1)
        label_times.rename(columns={"target": "label"}, inplace=True)
        threshold = p.operations[0].threshold

        if str(p) == "For each <id> predict the number of records in next 2d days":
            assert label_times["label"].tolist() == [1, 2, 2, 1]
            verify_label_times(
                cutoff_strategy,
                df,
                p,
                label_times,
                threshold=threshold,
                function=num_observations,
                operation=None,
            )
            problems_verified += 1
        elif "For each <id> predict the number of records with <amount>" in str(p):
            is_greater_than = False
            if "greater than" in str(p):
                is_greater_than = True

            if is_greater_than and threshold == 40:
                assert label_times["label"].tolist() == [0, 0, 1, 1]
            elif is_greater_than and threshold == 30:
                assert label_times["label"].tolist() == [0, 0, 2, 1]
            elif is_greater_than and threshold == 10:
                assert label_times["label"].tolist() == [0, 2, 2, 1]

            if not is_greater_than and threshold == 30:
                assert label_times["label"].tolist() == [1, 1, 0, 0]
            elif not is_greater_than and threshold == 40:
                assert label_times["label"].tolist() == [1, 2, 0, 0]
            elif not is_greater_than and threshold == 50:
                assert label_times["label"].tolist() == [1, 2, 1, 0]
            label_function = column_gt_len if is_greater_than else column_lt_len
            verify_label_times(
                cutoff_strategy,
                df,
                p,
                label_times,
                threshold=threshold,
                function=label_function,
                operation=None,
            )
            problems_verified += 1
        elif (
            "For each <id> predict the total <amount> in all related records in next 2d days"
            in str(p)
        ):
            assert label_times["label"].tolist() == [10, 50, 90, 60]
            verify_label_times(
                cutoff_strategy,
                df,
                p,
                label_times,
                threshold=threshold,
                function=sum_column,
                operation=None,
            )
            problems_verified += 1
        elif (
            "For each <id> predict the total <amount> in all related records with <amount> greater than"
            in str(p)
        ):
            if threshold == 40:
                assert label_times["label"].tolist() == [50, 60]
            elif threshold == 30:
                assert label_times["label"].tolist() == [90, 60]
            verify_label_times(
                cutoff_strategy,
                df,
                p,
                label_times,
                threshold=threshold,
                function=column_gt_op,
                operation="sum",
            )
            problems_verified += 1
        elif (
            "For each <id> predict the total <amount> in all related records with <amount> less than"
            in str(p)
        ):
            if threshold == 10:
                assert label_times["label"].tolist() == [10, 20]
            elif threshold == 20:
                assert label_times["label"].tolist() == [10, 20]
            verify_label_times(
                cutoff_strategy,
                df,
                p,
                label_times,
                threshold=threshold,
                function=column_lt_op,
                operation="sum",
            )
            problems_verified += 1
        elif (
            "For each <id> predict the average <amount> in all related records in next 2d days"
            in str(p)
        ):
            assert label_times["label"].tolist() == [10.0, 25.0, 45.0, 60.0]
            verify_label_times(
                cutoff_strategy,
                df,
                p,
                label_times,
                threshold=threshold,
                function=avg_column,
                operation=None,
            )
            problems_verified += 1
        elif (
            "For each <id> predict the average <amount> in all related records with <amount> greater than"
            in str(p)
        ):
            if threshold == 40:
                assert label_times["label"].tolist() == [50.0, 60.0]
            elif threshold == 30:
                assert label_times["label"].tolist() == [45.0, 60.0]
            verify_label_times(
                cutoff_strategy,
                df,
                p,
                label_times,
                threshold=threshold,
                function=column_gt_op,
                operation="mean",
            )
            problems_verified += 1
        elif (
            "For each <id> predict the average <amount> in all related records with <amount> less than"
            in str(p)
        ):
            if threshold == 30:
                assert label_times["label"].tolist() == [10.0, 20.0]
            elif threshold == 40:
                assert label_times["label"].tolist() == [10.0, 25.0]
            verify_label_times(
                cutoff_strategy,
                df,
                p,
                label_times,
                threshold=threshold,
                function=column_lt_op,
                operation="mean",
            )
            problems_verified += 1
        elif (
            "For each <id> predict the maximum <amount> in all related records in next 2d days"
            in str(p)
        ):
            assert label_times["label"].tolist() == [10.0, 30.0, 50.0, 60.0]
            verify_label_times(
                cutoff_strategy,
                df,
                p,
                label_times,
                threshold=threshold,
                function=max_column,
                operation="max",
            )
            problems_verified += 1
        elif (
            "For each <id> predict the maximum <amount> in all related records with <amount> greater than"
            in str(p)
        ):
            if threshold in [30, 40]:
                assert label_times["label"].tolist() == [50, 60]
            verify_label_times(
                cutoff_strategy,
                df,
                p,
                label_times,
                threshold=threshold,
                function=column_gt_op,
                operation="max",
            )
            problems_verified += 1
        elif (
            "For each <id> predict the maximum <amount> in all related records with <amount> less than"
            in str(p)
        ):
            if threshold in [10, 20]:
                assert label_times["label"].tolist() == [10, 20]
            verify_label_times(
                cutoff_strategy,
                df,
                p,
                label_times,
                threshold=threshold,
                function=column_lt_op,
                operation="max",
            )
            problems_verified += 1
        elif (
            "For each <id> predict the minimum <amount> in all related records in next 2d days"
            in str(p)
        ):
            assert label_times["label"].tolist() == [10, 20, 40, 60]
            verify_label_times(
                cutoff_strategy,
                df,
                p,
                label_times,
                threshold=threshold,
                function=min_column,
                operation="min",
            )
            problems_verified += 1
        elif (
            "For each <id> predict the minimum <amount> in all related records with <amount> greater than"
            in str(p)
        ):
            if threshold == 40:
                assert label_times["label"].tolist() == [50, 60]
            elif threshold == 30:
                assert label_times["label"].tolist() == [40, 60]
            verify_label_times(
                cutoff_strategy,
                df,
                p,
                label_times,
                threshold=threshold,
                function=column_gt_op,
                operation="min",
            )
            problems_verified += 1
        elif (
            "For each <id> predict the minimum <amount> in all related records with <amount> less than"
            in str(p)
        ):
            if threshold in [30, 40]:
                assert label_times["label"].tolist() == [10, 20]
            verify_label_times(
                cutoff_strategy,
                df,
                p,
                label_times,
                threshold=threshold,
                function=column_lt_op,
                operation="min",
            )
            problems_verified += 1
    assert problems_verified >= 35
    assert problems[0].__hash__() == problems[0].__hash__()
    assert problems[8].__hash__() != problems[9].__hash__()


def verify_label_times(
    cutoff_strategy,
    df,
    p,
    label_times,
    function,
    operation,
    threshold,
    column="amount",
):
    window_size = cutoff_strategy.window_size
    minimum_data = cutoff_strategy.minimum_data
    maximum_data = cutoff_strategy.maximum_data

    expected_label_times = generate_label_times(
        function,
        df,
        minimum_data,
        maximum_data,
        window_size,
        value=threshold,
        column=column,
        operation=operation,
    )
    pd.testing.assert_frame_equal(
        expected_label_times,
        label_times,
        check_frame_type=False,
    )


def generate_label_times(
    labeling_function,
    df,
    minimum_data,
    maximum_data,
    window_size,
    column=None,
    value=None,
    operation="sum",
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
        operation=operation,
        num_examples_per_instance=-1,
        minimum_data=minimum_data,
        maximum_data=maximum_data,
        verbose=False,
    )
    # rename the third column to label
    lt.rename(columns={lt.columns[2]: label_column_name}, inplace=True)
    return lt
