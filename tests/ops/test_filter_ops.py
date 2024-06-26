import pandas as pd
import pytest

from trane.ops.filter_ops import (
    AllFilterOp,
    EqFilterOp,
    GreaterFilterOp,
    LessFilterOp,
    NeqFilterOp,
)


@pytest.fixture
def df():
    df = pd.DataFrame({"col": [1, 2, 3, 4, 5]})
    return df


def test_all_filter_op(df):
    op = AllFilterOp("col")
    output = op(df)
    assert output["col"].tolist() == df["col"].tolist()
    assert op.generate_description() == ""


def test_all_filter_op_empty():
    op = AllFilterOp("col")
    df = pd.DataFrame()
    output = op(df)
    assert pd.isna(output)


def test_eq_filter_op(df):
    op = EqFilterOp("col")
    assert op.generate_description() == " with <col> equal to <str>"
    op.set_parameters(threshold=3)
    output = op(df)
    assert output["col"].tolist() == [3]
    assert op.generate_description() == " with <col> equal to <3>"


def test_neq_filter_op(df):
    op = NeqFilterOp("col")
    op.set_parameters(threshold=3)
    output = op(df)
    assert output["col"].tolist() == [1, 2, 4, 5]
    assert op.generate_description() == " with <col> not equal to <3>"


def test_less_filter_op(df):
    op = LessFilterOp("col")
    assert op.generate_description() == " with <col> less than <float>"
    op.set_parameters(threshold=3)
    output = op(df)
    assert output["col"].tolist() == [1, 2]
    assert op.generate_description() == " with <col> less than <3>"


def test_greater_filter_op(df):
    op = GreaterFilterOp("col")
    op.set_parameters(threshold=3)
    output = op(df)
    assert output["col"].tolist() == [4, 5]
    assert op.generate_description() == " with <col> greater than <3>"
