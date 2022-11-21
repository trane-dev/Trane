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


def test_count_aggregation_op_input_value():
    op = CountAggregationOp('col')
    op.op_type_check(meta)
    output = op(df.copy())
    assert output == 5


def test_sum_aggregation_op_input_value():
    op = SumAggregationOp('col')
    op.op_type_check(meta)
    output = op(df.copy())
    assert output == 15