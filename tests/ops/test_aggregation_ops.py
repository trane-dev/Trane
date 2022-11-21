from pandas import DataFrame

from trane.ops.aggregation_ops import (
    CountAggregationOp,
    SumAggregationOp,
    AvgAggregationOp,
    MaxAggregationOp,
    MinAggregationOp,
    MajorityAggregationOp
)
from trane.utils.table_meta import TableMeta as TM

df = DataFrame({'col': [1, 2, 3, 4, 5]})
meta = TM({
    "tables": [
        {"fields": [{'name': 'col', 'type': TM.SUPERTYPE[
            TM.TYPE_FLOAT], 'subtype': TM.TYPE_FLOAT}]}
    ]})


def test_count_aggregation_op():
    op = CountAggregationOp('col')
    op.op_type_check(meta)
    output = op(df.copy())
    assert output == 5


def test_sum_aggregation_op():
    op = SumAggregationOp('col')
    op.op_type_check(meta)
    output = op(df.copy())
    assert output == 15


def test_agg_aggregation_op():
    op = AvgAggregationOp('col')
    op.op_type_check(meta)
    output = op(df.copy())
    assert output == 3.00


def test_max_aggregation_op():
    op = MaxAggregationOp('col')
    op.op_type_check(meta)
    output = op(df.copy())
    assert output == 5


def test_min_aggregation_op():
    op = MinAggregationOp('col')
    op.op_type_check(meta)
    output = op(df.copy())
    assert output == 1


def test_majority_aggregation_op():
    op = MajorityAggregationOp('col')
    op.op_type_check(meta)
    output = op(df.copy())
    assert output == str(1)