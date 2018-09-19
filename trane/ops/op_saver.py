import json

from .aggregation_ops import *  # noqa
from .filter_ops import *  # noqa

__all__ = ["op_to_json", "op_from_json"]


def op_to_json(op):
    """
    Convert an operation object to a JSON string.

    Parameters
    ----------
    op: an operation object (subclass of OpBase)

    Returns
    ----------
    str: a JSON string representing the operation

    """
    return json.dumps({
        "OpType": op.__class__.__bases__[-1].__name__,
        "SubopType": type(op).__name__,
        "column_name": op.column_name,
        "iotype": (op.input_type, op.output_type),
        "hyper_parameter_settings": op.hyper_parameter_settings
    })


def op_from_json(json_data):
    """
    Convert an operation from a JSON string
    to an operation object.

    Parameters
    ----------
    json_data: the JSON string

    Returns
    ----------
    op: the operation object

    """
    data = json.loads(json_data)
    op = globals()[data['SubopType']](data['column_name'])
    op.input_type, op.output_type = data['iotype']
    op.hyper_parameter_settings = data['hyper_parameter_settings']
    return op
