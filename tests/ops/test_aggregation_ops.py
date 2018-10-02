from pandas import DataFrame
from trane.ops.aggregation_ops import *  # noqa
from trane.utils.table_meta import TableMeta as TM

df = DataFrame({'col': [1, 2, 3, 4, 5], 'col2': ['a', 'b', 'a', 'c', 'a']})
meta = TM({
    "tables": [
        {"fields": [
            {'name': 'col', 'type': TM.SUPERTYPE[
                TM.TYPE_FLOAT], 'subtype': TM.TYPE_FLOAT},
            {'name': 'col2', 'type': TM.SUPERTYPE[
                TM.TYPE_CATEGORY], 'subtype': TM.TYPE_CATEGORY},
        ]}
    ]})


def test_count_aggregation_op_input_value():
    op = CountAggregationOp(None)
    op.op_type_check(meta)
    output = op(df.copy())
    assert output == 5


def test_sum_aggregation_op_input_value():
    op = SumAggregationOp('col')
    op.op_type_check(meta)
    output = op(df.copy())
    assert output == 15


def test_avg_aggregation_op_input_value():
    op = AvgAggregationOp('col')
    op.op_type_check(meta)
    output = op(df.copy())
    assert output == 3


def test_majority_aggregation_op_input_value():
    op = MajorityAggregationOp('col2')
    op.op_type_check(meta)
    output = op(df.copy())
    assert output == 'a'
