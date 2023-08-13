import pandas as pd
import pytest

from trane.ops.transformation_ops import (
    OrderByOp,
    IdentityOp
)


@pytest.fixture
def df():
    df = pd.DataFrame({"col": [5, 3, 4, 1, 2]})
    return df


def test_identity_transformation_op(df):
    op = IdentityOp("col")
    output = op(df)
    assert output["col"].tolist() == df["col"].tolist()
    assert op.generate_description() == ""


def test_order_by_transformation_op_empty(df):
    op = OrderByOp("col")
    output = op(df)
    assert (output.values == df.sort_values(by="col").values).all()
    assert "sorted by <col>" in op.generate_description()