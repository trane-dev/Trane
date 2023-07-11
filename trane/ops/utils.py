from trane.ops.aggregation_ops import AggregationOpBase
from trane.ops.filter_ops import FilterOpBase


def get_aggregation_ops():
    return AggregationOpBase.__subclasses__()


def get_filter_ops():
    return FilterOpBase.__subclasses__()
