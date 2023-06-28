import pandas as pd
import pytest
from woodwork.column_schema import ColumnSchema

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


@pytest.mark.parametrize(
    "filter_operation,threshold,expected_values",
    [
        (AllFilterOp, None, [1, 2, 3, 4, 5]),
        (GreaterFilterOp, 2, [3, 4, 5]),
        (EqFilterOp, 3, [3]),
        (NeqFilterOp, 1, [2, 3, 4, 5]),
        (LessFilterOp, 4, [1, 2, 3]),
    ],
)
def test_agg_ops(df, filter_operation, threshold, expected_values):
    op = filter_operation("col")
    if threshold:
        op.hyper_parameter_settings["threshold"] = threshold
    if op.IOTYPES:
        assert isinstance(op.IOTYPES, list)
        for input_type, output_type in op.IOTYPES:
            assert isinstance(input_type, ColumnSchema)
            assert isinstance(output_type, ColumnSchema)
    output = op(df)
    # because filter check some criteria and return the rows which match the criteria
    # we do greater than, equal, not equal, less than, and all
    # so we are checking index values
    output = output["col"].tolist()
    assert output == expected_values
