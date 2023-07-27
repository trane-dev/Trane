import pandas as pd
import pytest

from trane.ops.aggregation_ops import (
    AvgAggregationOp,
    CountAggregationOp,
    MinAggregationOp,
)
from trane.ops.filter_ops import AllFilterOp, EqFilterOp, GreaterFilterOp, LessFilterOp


@pytest.fixture
def df():
    return pd.DataFrame(
        {
            "id": [1, 2, 2, 2, 3, 3, 3],
            "col": [10, 20, 30, 40, 50, 60, 70],
            "col2": ["red", "red", "blue", "blue", "blue", "green", "green"],
        },
    )


def test_find_threshold_by_fraction_of_data_to_keep(df):
    op = GreaterFilterOp("col")
    original_threshold = 20.0
    op.set_parameters(threshold=original_threshold)
    best_threshold = op.find_threshold_by_fraction_of_data_to_keep(
        fraction_of_data_target=0.25,
        df=df,
        label_col="col",
        random_state=0,
    )
    # want to keep 0.25 of the data (25%)
    # 50 is the lowest number that keeps 25% of the data (length is 7, keep 2 numbers)
    assert best_threshold == 50.0
    assert op.threshold == original_threshold
    assert hash(op) == hash(("GreaterFilterOp", "col", original_threshold))


def test_hash_no_threshold():
    assert hash(CountAggregationOp(None)) == hash(CountAggregationOp(None))
    assert hash(CountAggregationOp(None)) != hash(CountAggregationOp("col"))
    assert hash(CountAggregationOp(None)) != hash(EqFilterOp("col"))


def test_hash_categorical_threshold():
    op_1 = EqFilterOp("col")
    op_1.set_parameters(threshold="NY")
    op_2 = EqFilterOp("col")
    op_2.set_parameters(threshold="NJ")
    assert hash(op_1) != hash(op_2)


def test_hash_numeric_threshold():
    op_1 = GreaterFilterOp("col")
    op_1.set_parameters(threshold=20.0)
    op_2 = GreaterFilterOp("col")
    op_2.set_parameters(threshold=50.0)
    assert hash(op_1) != hash(op_2)


def test_repr():
    assert str(AllFilterOp) == "AllFilterOp"
    assert AllFilterOp(None).__repr__() == "AllFilterOp"
    assert GreaterFilterOp("col").__repr__() == "GreaterFilterOp(col)"
    assert GreaterFilterOp("col").__str__() == "GreaterFilterOp(col)"
    assert str(MinAggregationOp("col")) == "MinAggregationOp(col)"


def test_eq():
    assert GreaterFilterOp("col") == GreaterFilterOp("col")
    assert GreaterFilterOp("col") != LessFilterOp("col")


def test_lt():
    assert GreaterFilterOp("col") < LessFilterOp("col")
    assert GreaterFilterOp("col") <= GreaterFilterOp("col")
    assert MinAggregationOp("col") > AvgAggregationOp("col")
    assert MinAggregationOp("col") >= MinAggregationOp("col")
