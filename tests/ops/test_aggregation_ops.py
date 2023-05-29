import numpy as np
import pandas as pd
import pytest
from woodwork.column_schema import ColumnSchema

from trane.ops.aggregation_ops import (
    AvgAggregationOp,
    CountAggregationOp,
    MajorityAggregationOp,
    MaxAggregationOp,
    MinAggregationOp,
    SumAggregationOp,
)


@pytest.fixture
def df():
    df = pd.DataFrame({"col": [1, 2, 3, 4, 5]})
    return df


@pytest.mark.parametrize(
    "agg_operation,expected_output",
    [
        (CountAggregationOp, 5),
        (SumAggregationOp, 15),
        (AvgAggregationOp, 3.00),
        (MaxAggregationOp, 5),
        (MinAggregationOp, 1),
        (MajorityAggregationOp, str(1)),
    ],
)
def test_agg_ops(df, agg_operation, expected_output):
    op = agg_operation("col")
    output = op(df)
    assert output == expected_output
    if agg_operation == MajorityAggregationOp:
        assert isinstance(output, str)
    else:
        assert isinstance(output, float) or isinstance(output, (int, np.integer))
    if hasattr(op, "OTYPE"):
        assert op.OTYPE == ColumnSchema(semantic_tags={"numeric"})


@pytest.mark.parametrize(
    "agg_operation",
    [
        (SumAggregationOp),
        (AvgAggregationOp),
        (MaxAggregationOp),
        (MinAggregationOp),
        (MajorityAggregationOp),
    ],
)
def test_sum_agg_none(agg_operation):
    op = agg_operation("col")
    df = pd.DataFrame()
    output = op(df)
    assert output is None
