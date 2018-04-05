from trane.ops.op_base import OpBase
from trane.utils.table_meta import TableMeta as TM
from trane.ops.row_ops import *
from trane.ops.transformation_ops import *
import pytest


class FakeOp(OpBase):
    """
    Make a fake operation for testing. 
    It has PARAMS and IOTYPES, but execute is not implemented
    """
    PARAMS = [{'param': TM.TYPE_FLOAT}, {'param': TM.TYPE_TEXT}]
    IOTYPES = [(TM.TYPE_FLOAT, TM.TYPE_BOOL), (TM.TYPE_TEXT, TM.TYPE_BOOL)]


def test_op_base_init():
    """
    Check if FakeOp is initialized correctly.
    Check if NotImplementedError is raised.
    """
    op = FakeOp('col')
    assert op.input_type is None
    assert op.output_type is None
    assert type(op.hyper_parameter_settings) == dict
    with pytest.raises(NotImplementedError):
        op(None)


def test_op_type_check_with_correct_type1():
    """
    With input type TYPE_FLOAT, check if input_type and output_type are correct.
    """
    meta = TM({
        "tables": [
            {"fields": [{'name': 'col', 'type': TM.SUPERTYPE[
                TM.TYPE_FLOAT], 'subtype': TM.TYPE_FLOAT}]}
        ]})
    op = FakeOp('col')
    meta2 = op.op_type_check(meta)
    assert meta2 == meta
    assert meta.get_type('col') == TM.TYPE_BOOL
    assert op.input_type == TM.TYPE_FLOAT and op.output_type == TM.TYPE_BOOL


def test_op_type_check_with_correct_type2():
    """
    With input type TYPE_TEXT, check if input_type and output_type are correct.
    """
    meta = TM({
        "tables": [
            {"fields": [{'name': 'col', 'type': TM.TYPE_TEXT}]}
        ]})
    op = FakeOp('col')
    meta2 = op.op_type_check(meta)
    assert meta2 == meta
    assert meta.get_type('col') == TM.TYPE_BOOL
    assert op.input_type == TM.TYPE_TEXT and op.output_type == TM.TYPE_BOOL


def test_op_type_check_with_wrong_type():
    """
    with input type TYPE_IDENTIFIER, check if None is returned by op_type_check.
    """
    meta = TM({
        "tables": [
            {"fields": [{'name': 'col', 'type': TM.TYPE_IDENTIFIER}]}
        ]})
    op = FakeOp('col')
    meta2 = op.op_type_check(meta)
    assert meta2 is None
    assert meta.get_type('col') == TM.TYPE_IDENTIFIER
    assert op.input_type is TM.TYPE_IDENTIFIER and op.output_type is None


def test_op_equality():
    column_name = "test"
    id_row_op = IdentityRowOp(column_name)
    id_row_op_clone = IdentityRowOp(column_name)
    assert(id_row_op == id_row_op_clone)

    id_trans_op = IdentityTransformationOp(column_name)
    id_trans_op_clone = IdentityTransformationOp(column_name)

    assert(id_trans_op == id_trans_op_clone)
