import pandas as pd
import pytest

from trane.ops.aggregation_ops import (
    AvgAggregationOp,
    CountAggregationOp,
    MajorityAggregationOp,
    MaxAggregationOp,
    MinAggregationOp,
    SumAggregationOp,
)
from trane.utils.table_meta import TableMeta


@pytest.fixture
def df():
    df = pd.DataFrame({"col": [1, 2, 3, 4, 5]})
    return df


@pytest.fixture
def meta():
    meta = TableMeta(
        {
            "tables": [
                {
                    "fields": [
                        {
                            "name": "col",
                            "type": TableMeta.SUPERTYPE[TableMeta.TYPE_FLOAT],
                            "subtype": TableMeta.TYPE_FLOAT,
                        },
                    ],
                },
            ],
        },
    )
    return meta


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
def test_agg_ops(df, meta, agg_operation, expected_output):
    op = agg_operation("col")
    op.op_type_check(meta)
    output = op(df)
    assert output == expected_output


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
