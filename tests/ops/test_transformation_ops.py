from trane.ops.transformation_ops import *
from pandas import DataFrame
from trane.utils.table_meta import TableMeta as TM
import numpy as np

df = DataFrame({'col': [1, 2, 3, 4, 5]})
meta = TM([{'name': 'col', 'type': TM.TYPE_VALUE}])

def test_identity_transformation_op_input_value():
    op = IdentityTransformationOp('col')
    op.preprocess(meta)
    output = op(df.copy())
    assert np.all(output.values == np.asarray([[1, 2, 3, 4, 5]]).T)
    
def test_diff_transformation_op_input_value():
    op = DiffTransformationOp('col')
    op.preprocess(meta)
    output = op(df.copy())
    print(output)
    assert np.all(output.values == np.asarray([[0, 1, 1, 1, 1]]).T)
