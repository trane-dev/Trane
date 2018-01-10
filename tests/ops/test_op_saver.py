from trane.ops import *
from trane.utils.table_meta import TableMeta as TM
import pytest
import numpy as np

ALL_OPS = AGGREGATION_OPS + FILTER_OPS + ROW_OPS + TRANSFORMATION_OPS

def test_save_load_before_preprocess():
    """
    Randomly select 10 operations, check to_json and from_json work properly before preprocess
    """
    for i in range(10):
        op_type = np.random.choice(ALL_OPS)
        op = globals()[op_type]('col')
        op_json = to_json(op)
        assert type(op_json) == str
        op2 = from_json(op_json)
        assert type(op2) == globals()[op_type]
        assert op2.column_name == 'col'
        assert op2.itype is None and op2.otype is None
        assert type(op2.param_values) == dict and len(op2.param_values) == 0

def test_save_load_after_preprocess():
    """
    Randomly select 10 operations, check to_json and from_json work properly after preprocess
    """
    for i in range(10):
        meta = TM([{'name': 'col', 'type': TM.TYPE_VALUE}])
        op_type = np.random.choice(ALL_OPS)
        op = globals()[op_type]('col')
        op.preprocess(meta)
        op_json = to_json(op)
        assert type(op_json) == str
        op2 = from_json(op_json)
        assert type(op2) == globals()[op_type]
        assert op2.column_name == 'col'
        assert op2.itype == TM.TYPE_VALUE and op2.otype == op.otype
        assert type(op2.param_values) == dict
