from trane.ops.row_ops import *
from pandas import DataFrame
from trane.utils.table_meta import TableMeta as TM
import numpy as np

df = DataFrame({'col': [1, 2, 3, 4, 5]})
meta = TM([{'name': 'col', 'type': TM.TYPE_VALUE}])

def test_identity_row_op_input_value():
    op = IdentityRowOp('col')
    op.preprocess(meta)
    output = op(df.copy())
    assert np.all(output.values == np.asarray([[1, 2, 3, 4, 5]]).T)
    
def test_greater_row_op_input_value():
    op = GreaterRowOp('col')
    op.preprocess(meta)
    op.param_values['threshold'] = 2
    output = op(df.copy())
    assert np.all(output.values == np.asarray([[False, False, True, True, True]]).T)
