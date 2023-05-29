import pandas as pd
import pytest
from woodwork.column_schema import ColumnSchema
from woodwork.logical_types import (
    Boolean,
    Categorical,
    Datetime,
    Double,
)

from trane.ops.op_base import OpBase


class FakeOp(OpBase):
    REQUIRED_PARAMETERS = []
    IOTYPES = [
        (
            ColumnSchema(logical_type=Double, semantic_tags={"numeric"}),
            ColumnSchema(logical_type=Boolean),
        ),
    ]


class FakeOpRequired(OpBase):
    REQUIRED_PARAMETERS = [{"threshold": None}]
    IOTYPES = [
        (
            ColumnSchema(logical_type=Categorical),
            ColumnSchema(logical_type=Boolean),
        ),
    ]


def test_op_base_init():
    """
    Check if FakeOp is initialized correctly.
    Check if NotImplementedError is raised.
    """
    op = FakeOp("col")
    assert op.input_type is None
    assert op.output_type is None
    assert isinstance(op.hyper_parameter_settings, dict)
    assert len(op.hyper_parameter_settings) == 0
    with pytest.raises(NotImplementedError):
        op(None)


def test_op_type_check_returns_modified_meta():
    meta = {
        "id": ColumnSchema(logical_type=Categorical, semantic_tags={"index"}),
        "time": ColumnSchema(logical_type=Datetime, semantic_tags={"time_index"}),
        "price": ColumnSchema(logical_type=Double, semantic_tags={"numeric"}),
        "product": ColumnSchema(logical_type=Categorical, semantic_tags={"category"}),
    }
    expected_meta = {
        "id": ColumnSchema(logical_type=Categorical, semantic_tags={"index"}),
        "time": ColumnSchema(logical_type=Datetime, semantic_tags={"time_index"}),
        "price": ColumnSchema(logical_type=Boolean),
        "product": ColumnSchema(logical_type=Categorical, semantic_tags={"category"}),
    }
    op = FakeOp("price")
    output_meta = op.op_type_check(meta)
    assert output_meta == expected_meta
    assert op.input_type == ColumnSchema(logical_type=Double, semantic_tags={"numeric"})
    assert op.output_type == ColumnSchema(logical_type=Boolean)


def test_op_type_check_wrong_type():
    meta = {
        "id": ColumnSchema(logical_type=Categorical, semantic_tags={"index"}),
        "time": ColumnSchema(logical_type=Datetime, semantic_tags={"time_index"}),
        "price": ColumnSchema(logical_type=Double, semantic_tags={"numeric"}),
        "product": ColumnSchema(logical_type=Categorical, semantic_tags={"category"}),
    }
    op = FakeOp("product")
    output_meta = op.op_type_check(meta)
    assert output_meta is None


def test_set_hyper_parameter():
    op = FakeOpRequired("col")
    op.set_hyper_parameter(parameter_name="threshold", parameter_value=5)
    assert op.hyper_parameter_settings["threshold"] == 5


def test_set_hyper_parameter_raises():
    op = FakeOpRequired("col")
    with pytest.raises(ValueError):
        op.set_hyper_parameter(parameter_name="invalid_param", parameter_value=5)


def test_sample_df_and_unique_values():
    values = [1, 2, 2, 4, 4, 5]
    df = pd.DataFrame({"col": values})
    op = FakeOpRequired("col")
    max_num_rows = 3
    max_num_unique_values = len(set(values))
    sample_df, unique_vals = op._sample_df_and_unique_values(
        df=df,
        col="col",
        max_num_unique_values=max_num_unique_values,
        max_num_rows=max_num_rows,
    )
    assert len(sample_df["col"].unique()) <= max_num_unique_values
    assert sample_df.shape == (max_num_rows, 1)
    assert unique_vals == set(values)


# def test_op_equality():
#     column_name = "test"
#     id_row_op = IdentityRowOp(column_name)
#     id_row_op_clone = IdentityRowOp(column_name)
#     assert(id_row_op == id_row_op_clone)

#     id_trans_op = IdentityTransformationOp(column_name)
#     id_trans_op_clone = IdentityTransformationOp(column_name)

#     assert(id_trans_op == id_trans_op_clone)
