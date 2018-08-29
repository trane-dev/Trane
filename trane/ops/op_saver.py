import json

from .aggregation_ops import *  # noqa
from .filter_ops import *  # noqa
from .row_ops import *  # noqa
from .transformation_ops import *  # noqa


def op_from_json(data):
    """
    Convert an operation from a JSON string
    to an operation object.

    Parameters
    ----------
    data: the JSON string

    Returns
    ----------
    op: the operation object

    """
    if type(data) != dict:
        data = json.loads(data)

    op = globals()[data['SubopType']](data['column_name'])
    op.input_type, op.output_type = data['iotype']
    op.hyper_parameter_settings = data['hyper_parameter_settings']
    return op
