from trane.ops.aggregation_ops import *
from pandas import DataFrame
from trane.utils.table_meta import TableMeta as TM

df = DataFrame({'col': [1, 2, 3, 4, 5]})
meta = TM({
    "tables": [
        {"fields": [{'name': 'col', 'type': TM.SUPERTYPE[
            TM.TYPE_FLOAT], 'subtype': TM.TYPE_FLOAT}]}
    ]})


def test_first_aggregation_op_input_value():
    op = FirstAggregationOp('col')
    op.op_type_check(meta)
    output = op(df.copy())
    assert len(output) == 1
    assert output.values[0, 0] == 1


def test_last_aggregation_op_input_value():
    op = LastAggregationOp('col')
    op.op_type_check(meta)
    output = op(df.copy())
    assert len(output) == 1
    assert output.values[0, 0] == 5


def test_lmf_aggregation_op_input_value():
    op = LMFAggregationOp('col')
    op.op_type_check(meta)
    output = op(df.copy())
    assert len(output) == 1
    assert output.values[0, 0] == 4


def test_count_aggregation_op_input_value():
    op = CountAggregationOp('col')
    op.op_type_check(meta)
    output = op(df.copy())
    assert len(output) == 1
    assert output.values[0, 0] == 5


def test_sum_aggregation_op_input_value():
    op = SumAggregationOp('col')
    op.op_type_check(meta)
    output = op(df.copy())
    assert len(output) == 1
    assert output.values[0, 0] == 15
