import pandas as pd
import pytest

from trane.ops.filter_ops import GreaterFilterOp, LessFilterOp


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
    assert hash(op) == hash(("GreaterFilterOp", "col"))


def test_eq():
    assert GreaterFilterOp("col") == GreaterFilterOp("col")
    assert GreaterFilterOp("col") != LessFilterOp("col")


def test_lt():
    assert GreaterFilterOp("col") < LessFilterOp("col")
