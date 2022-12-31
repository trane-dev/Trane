import pandas as pd
import pytest

from trane.ops.op_base import OpBase
from trane.utils.table_meta import TableMeta as TM


class FakeOp(OpBase):
    """
    Make a fake operation for testing.
    It has PARAMS and IOTYPES, but execute is not implemented
    """

    PARAMS = [{"param": TM.TYPE_FLOAT}, {"param": TM.TYPE_TEXT}]
    IOTYPES = [(TM.TYPE_FLOAT, TM.TYPE_BOOL), (TM.TYPE_TEXT, TM.TYPE_BOOL)]


class FakeOpRequired(OpBase):
    REQUIRED_PARAMETERS = [{"threshold": TM.TYPE_CATEGORY}]
    IOTYPES = None


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


def test_op_type_check_with_correct_type1():
    """
    With input type TYPE_FLOAT, check if input_type and output_type are correct.
    """
    meta = TM(
        {
            "tables": [
                {
                    "fields": [
                        {
                            "name": "col",
                            "type": TM.SUPERTYPE[TM.TYPE_FLOAT],
                            "subtype": TM.TYPE_FLOAT,
                        },
                    ],
                },
            ],
        },
    )
    op = FakeOp("col")
    meta2 = op.op_type_check(meta)
    assert meta2 == meta
    assert meta.get_type("col") == TM.TYPE_BOOL
    assert op.input_type == TM.TYPE_FLOAT and op.output_type == TM.TYPE_BOOL


def test_op_type_check_with_correct_type2():
    """
    With input type TYPE_TEXT, check if input_type and output_type are correct.
    """
    meta = TM({"tables": [{"fields": [{"name": "col", "type": TM.TYPE_TEXT}]}]})
    op = FakeOp("col")
    meta2 = op.op_type_check(meta)
    assert meta2 == meta
    assert meta.get_type("col") == TM.TYPE_BOOL
    assert op.input_type == TM.TYPE_TEXT and op.output_type == TM.TYPE_BOOL


def test_op_type_check_with_wrong_type():
    """
    with input type TYPE_IDENTIFIER, check if None is returned by op_type_check.
    """
    meta = TM({"tables": [{"fields": [{"name": "col", "type": TM.TYPE_IDENTIFIER}]}]})
    op = FakeOp("col")
    meta2 = op.op_type_check(meta)
    assert meta2 is None
    assert meta.get_type("col") == TM.TYPE_IDENTIFIER
    assert op.input_type is TM.TYPE_IDENTIFIER and op.output_type is None


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
