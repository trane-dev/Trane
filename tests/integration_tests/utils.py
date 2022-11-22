from composeml import LabelMaker

import trane
from trane.ops.aggregation_ops import (
    AggregationOpBase,
    AvgAggregationOp,
    CountAggregationOp,
    MajorityAggregationOp,
    MaxAggregationOp,
    MinAggregationOp,
    SumAggregationOp,
)
from trane.ops.filter_ops import (
    AllFilterOp,
    EqFilterOp,
    FilterOpBase,
    GreaterFilterOp,
    LessFilterOp,
    NeqFilterOp,
)

agg_op_str_dict = {
    SumAggregationOp: " the total <{}> in all related records",
    AvgAggregationOp: " the average <{}> in all related records",
    MaxAggregationOp: " the maximum <{}> in all related records",
    MinAggregationOp: " the minimum <{}> in all related records",
    MajorityAggregationOp: " the majority <{}> in all related records",
    CountAggregationOp: "the number of records",
}

filter_op_str_dict = {
    GreaterFilterOp: "greater than",
    EqFilterOp: "equal to",
    NeqFilterOp: "not equal to",
    LessFilterOp: "less than",
    # TODO: figure out the string for this operation
    AllFilterOp: "",
}


def generate_and_verify_prediction_problem(
    df, meta, entity_col, time_col, cutoff_strategy
):
    cutoff = cutoff_strategy.window_size
    problem_generator = trane.PredictionProblemGenerator(
        table_meta=meta,
        entity_col=entity_col,
        cutoff_strategy=cutoff_strategy,
        time_col=time_col,
    )
    problems = problem_generator.generate(df, generate_thresholds=True)
    unique_entity_ids = df[entity_col].nunique()
    for p in problems:
        assert p.entity_col == entity_col
        assert p.time_col == time_col
        assert isinstance(p._label_maker, LabelMaker)
        expected_problem_pre = f"For each <{entity_col}> predict the"
        expected_problem_end = f"in next {cutoff} days"
        p_str = str(p)
        assert p_str.startswith(expected_problem_pre)
        assert p_str.endswith(expected_problem_end)
        agg_column_name = None
        filter_column_name = None
        filter_threshold_value = None    
        for op in p.operations:
            if isinstance(op, AggregationOpBase):
                expected_agg_str = agg_op_str_dict[op.__class__]
                expected_agg_str = expected_agg_str.replace(
                    "<{}>", f"<{op.column_name}>"
                )
                assert expected_agg_str in p_str
                if op.column_name:
                    agg_column_name = op.column_name
            elif isinstance(op, FilterOpBase):
                expected_filter_str = filter_op_str_dict[op.__class__]
                threshold = op.hyper_parameter_settings.get('threshold', None)
                assert expected_filter_str in p_str
                if op.column_name:
                    filter_column_name = op.column_name
                if threshold:
                    filter_threshold_value = threshold
            else:
                raise ValueError(f'Unexpected prediction problem generated: {p_str}: {p.operations}')
        label_times = p.execute(df, -1)
        assert label_times.target_dataframe_name == entity_col
        # TODO: fix bug with Filter Operation results in labels that has _execute_operations_on_df == 0
        # Below is not an ideal way to check the prediction problems 
        # (because it has less than, rather than exact number of unique instances)
        if not label_times.empty:
            assert label_times[entity_col].nunique() <= unique_entity_ids