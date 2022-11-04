import trane
from trane.ops.filter_ops import (FilterOpBase,
                                  GreaterFilterOp,
                                  EqFilterOp,
                                  NeqFilterOp,
                                  AllFilterOp,
                                  LessFilterOp)
from trane.ops.aggregation_ops import (AggregationOpBase, 
                                       CountAggregationOp,
                                       SumAggregationOp, 
                                       AvgAggregationOp,
                                       MaxAggregationOp,
                                       MinAggregationOp,
                                       MajorityAggregationOp)
from composeml import LabelMaker

agg_op_str_dict = {
    SumAggregationOp: " the total <{}> in all related records",
    AvgAggregationOp: " the average <{}> in all related records",
    MaxAggregationOp: " the maximum <{}> in all related records",
    MinAggregationOp: " the minimum <{}> in all related records",
    MajorityAggregationOp: " the majority <{}> in all related records",
    CountAggregationOp: 'the number of records',
}

filter_op_str_dict = {
    GreaterFilterOp: "greater than",
    EqFilterOp: "equal to",
    NeqFilterOp: "not equal to",
    LessFilterOp: "less than",
    # TODO: figure out the string for this operation
    AllFilterOp: ''
}

def generate_and_verify_prediction_problem(df, meta, entity_col, time_col, cutoff_strategy):
    cutoff = cutoff_strategy.window_size
    problem_generator = trane.PredictionProblemGenerator(table_meta=meta, 
                                                         entity_col=entity_col,
                                                         cutoff_strategy=cutoff_strategy,
                                                         time_col=time_col)
    problems = problem_generator.generate(df, generate_thresholds=True)
    for p in problems:
        assert p.entity_col == entity_col
        assert p.time_col == time_col 
        assert isinstance(p._label_maker, LabelMaker)
        expected_problem_pre = f'For each <{entity_col}> predict the'
        expected_problem_end = f'in next {cutoff} days'
        p_str = str(p)
        assert p_str.startswith(expected_problem_pre)
        assert p_str.endswith(expected_problem_end)
        for op in p.operations:
            if isinstance(op, AggregationOpBase):
                expected_agg_str = agg_op_str_dict[op.__class__]
                expected_agg_str = expected_agg_str.replace("<{}>", f"<{op.column_name}>")
                assert expected_agg_str in p_str
            elif isinstance(op, FilterOpBase):
                expected_filter_str = filter_op_str_dict[op.__class__]
                assert expected_filter_str in p_str
            else:
                print(p_str)
                print(p.operations)
	# for p in problems:
	# 	try:
	# 		x = p.execute(df,-1)
	# 		problem_label_dict[str(p)]=x
	# 	except:
	# 		pass