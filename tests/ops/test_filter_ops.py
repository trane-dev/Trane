import numpy as np
from pandas import DataFrame
from trane.ops.filter_ops import *  # noqa
from trane.utils.table_meta import TableMeta as TM

df = DataFrame({'col': [1, 2, 3, 4, 5], 'col2': ['a', 'b', 'a', 'c', 'b']})
meta = TM({
    "tables": [
        {"fields": [
            {'name': 'col', 'type': TM.SUPERTYPE[
                TM.TYPE_FLOAT], 'subtype': TM.TYPE_FLOAT},
            {'name': 'col2', 'type': TM.SUPERTYPE[
                TM.TYPE_CATEGORY], 'subtype': TM.TYPE_CATEGORY},
        ]}
    ]})


def test_all_filter_op_input_value():
    op = AllFilterOp(None)
    op.op_type_check(meta)
    output = op(df.copy())
    assert np.all(output.values == df.values)


def test_eq_filter_op_input_value():
    op = EqFilterOp('col2')
    op.op_type_check(meta)
    op.hyper_parameter_settings['threshold'] = 'a'
    output = op(df.copy())
    assert np.all(output.values == np.asarray([[1, 'a'], [3, 'a']], dtype='object'))


def test_neq_filter_op_input_value():
    op = NeqFilterOp('col2')
    op.op_type_check(meta)
    op.hyper_parameter_settings['threshold'] = 'a'
    output = op(df.copy())
    assert np.all(output.values == np.asarray([[2, 'b'], [4, 'c'], [5, 'b']], dtype='object'))


def test_greater_filter_op_input_value():
    op = GreaterFilterOp('col')
    op.op_type_check(meta)
    op.hyper_parameter_settings['threshold'] = 2
    output = op(df.copy())
    assert np.all(output.values == np.asarray([[3, 4, 5], ['a', 'c', 'b']], dtype='object').T)


def test_less_filter_op_input_value():
    op = LessFilterOp('col')
    op.op_type_check(meta)
    op.hyper_parameter_settings['threshold'] = 2
    output = op(df.copy())
    assert np.all(output.values == np.asarray([[1], ['a']], dtype='object').T)
