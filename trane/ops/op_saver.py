from .op_base import OpBase
from .aggregation_ops import *
from .filter_ops import *
from .row_ops import *
from .transformation_ops import *
import json

__all__ = ["to_json", "from_json"]

def to_json(op):
    return json.dumps({
        "OpType": op.__class__.__bases__[-1].__name__,
        "SubopType": type(op).__name__,
        "column_name": op.column_name,
        "iotype": (op.itype, op.otype),
        "param_values": op.param_values
    })

def from_json(json_data):
    data = json.loads(json_data)
    op = globals()[data['SubopType']](data['column_name'])
    op.itype, op.otype = data['iotype']
    op.param_values = data['param_values']
    return op
