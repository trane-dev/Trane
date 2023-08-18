import numpy as np
import pandas as pd
import pytest

from trane.ops.aggregation_ops import (
    AvgAggregationOp,
    CountAggregationOp,
    FirstAggregationOp,
    LastAggregationOp,
    MajorityAggregationOp,
    MaxAggregationOp,
    MinAggregationOp,
    SumAggregationOp,
)
from trane.ops.utils import get_aggregation_ops


@pytest.fixture
def df():
    df = pd.DataFrame({"col": [1, 2, 3, 4, 5]})
    return df


def test_count_agg_op(df):
    op = CountAggregationOp("col")
    output = op(df)
    assert output == df.shape[0]
    assert "the number of records" in op.generate_description()


def test_sum_agg_op(df):
    op = CountAggregationOp("col")
    op = SumAggregationOp("col")
    output = op(df)
    assert output == np.sum(df["col"])
    assert "the total <col> in all related records" in op.generate_description()


def test_avg_agg_op(df):
    op = AvgAggregationOp("col")
    output = op(df)
    assert output == np.average(df["col"])
    assert "the average <col> in all related records" in op.generate_description()


def test_max_agg_op(df):
    op = MaxAggregationOp("col")
    output = op(df)
    assert output == np.max(df["col"])
    assert "the maximum <col> in all related records" in op.generate_description()


def test_min_agg_op(df):
    op = MinAggregationOp("col")
    output = op(df)
    assert output == np.min(df["col"])
    assert "the minimum <col> in all related records" in op.generate_description()


def test_first_agg_op(df):
    op = FirstAggregationOp("col")
    output = op(df)
    assert output == df["col"].iloc[0]
    assert "the first <col> in all related records" in op.generate_description()


def test_last_agg_op(df):
    op = LastAggregationOp("col")
    output = op(df)
    assert output == df["col"].iloc[-1]
    assert "the last <col> in all related records" in op.generate_description()


@pytest.mark.parametrize(
    "dtype",
    [
        ("string"),
        ("string[pyarrow]"),
        ("int64"),
        ("int64[pyarrow]"),
        ("float64"),
        ("float64[pyarrow]"),
    ],
)
def test_majority_agg_op(df, dtype):
    op = MajorityAggregationOp("col")
    df["col"] = df["col"].astype(dtype)
    output = op(df)
    if dtype in ["string", "string[pyarrow]"]:
        assert output == str(1)
    elif dtype in ["int64", "int64[pyarrow]"]:
        assert output == int(1)
    else:
        assert output == float(1.0)
    assert "the majority <col> in all related records" in op.generate_description()


def test_agg_None():
    for agg_operation in get_aggregation_ops():
        op = agg_operation("col")
        df = pd.DataFrame()
        output = op(df)
        assert output in [None, 0]
        assert op.generate_description() is not None
