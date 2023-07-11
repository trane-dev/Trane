from trane.ops.aggregation_ops import (
    AggregationOpBase,
    AvgAggregationOp,
    CountAggregationOp,
    ExistsAggregationOp,
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
from trane.ops.utils import (
    get_aggregation_ops,
    get_filter_ops,
)


def test_get_aggregation_ops():
    for instance in get_aggregation_ops():
        assert issubclass(instance, AggregationOpBase)
    assert get_aggregation_ops() == [
        CountAggregationOp,
        SumAggregationOp,
        AvgAggregationOp,
        MaxAggregationOp,
        MinAggregationOp,
        MajorityAggregationOp,
        ExistsAggregationOp,
    ]


def test_get_filter_ops():
    for instance in get_filter_ops():
        assert issubclass(instance, FilterOpBase)
    assert get_filter_ops() == [
        AllFilterOp,
        EqFilterOp,
        NeqFilterOp,
        GreaterFilterOp,
        LessFilterOp,
    ]
