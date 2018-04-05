from trane.ops.filter_ops import *
from pandas import DataFrame
from trane.utils.table_meta import TableMeta as TM
import numpy as np

df = DataFrame({'col': [1, 2, 3, 4, 5]})
meta = TM({
    "tables": [
        {"fields": [{'name': 'col', 'type': TM.SUPERTYPE[
            TM.TYPE_FLOAT], 'subtype': TM.TYPE_FLOAT}]}
    ]})


def test_all_filter_op_input_value():
    op = AllFilterOp('col')
    op.op_type_check(meta)
    output = op(df.copy())
    assert np.all(output.values == np.asarray([[1, 2, 3, 4, 5]]).T)


def test_eq_filter_op_input_value():
    op = EqFilterOp('col')
    op.op_type_check(meta)
    op.hyper_parameter_settings['threshold'] = 2
    output = op(df.copy())
    assert np.all(output.values == np.asarray([[2]]).T)


def test_neq_filter_op_input_value():
    op = NeqFilterOp('col')
    op.op_type_check(meta)
    op.hyper_parameter_settings['threshold'] = 2
    output = op(df.copy())
    assert np.all(output.values == np.asarray([[1, 3, 4, 5]]).T)


def test_greater_filter_op_input_value():
    op = GreaterFilterOp('col')
    op.op_type_check(meta)
    op.hyper_parameter_settings['threshold'] = 2
    output = op(df.copy())
    assert np.all(output.values == np.asarray([[3, 4, 5]]).T)


def test_less_filter_op_input_value():
    op = LessFilterOp('col')
    op.op_type_check(meta)
    op.hyper_parameter_settings['threshold'] = 2
    output = op(df.copy())
    assert np.all(output.values == np.asarray([[1]]).T)
