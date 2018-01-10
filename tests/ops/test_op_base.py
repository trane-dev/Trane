from trane.ops.op_base import OpBase
from trane.utils.table_meta import TableMeta as TM
import pytest

class FakeOp(OpBase):
    PARAMS = [{'param': TM.TYPE_VALUE}, {'param': TM.TYPE_TEXT}]
    IOTYPES = [{TM.TYPE_VALUE, TM.TYPE_BOOL}, {TM.TYPE_TEXT, TM.TYPE_BOOL}]

def test_op_base_init():
    op = FakeOp('col')
    assert op.itype is None
    assert op.otype is None
    assert type(op.param_values) == dict
    with pytest.raises(NotImplementedError):
        op(None)

def test_preprocess():
    meta = TM([{'name': 'col', 'type': TM.TYPE_VALUE}])
    op = FakeOp('col')
    meta2 = op.preprocess(meta)
    assert meta2 == meta
    assert meta.get_type('col') == TM.TYPE_BOOL
    assert op.itype == TM.TYPE_VALUE and op.otype == TM.TYPE_BOOL
    
    meta = TM([{'name': 'col', 'type': TM.TYPE_TEXT}])
    op = FakeOp('col')
    meta2 = op.preprocess(meta)
    assert meta2 == meta
    assert meta.get_type('col') == TM.TYPE_BOOL
    assert op.itype == TM.TYPE_TEXT and op.otype == TM.TYPE_BOOL    
    
    meta = TM([{'name': 'col', 'type': TM.TYPE_IDENTIFIER}])
    op = FakeOp('col')
    meta2 = op.preprocess(meta)
    assert meta2 is None
    assert meta.get_type('col') == TM.TYPE_IDENTIFIER
    assert op.itype is TM.TYPE_IDENTIFIER and op.otype is None
    
