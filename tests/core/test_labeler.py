import numpy as np
import pandas as pd

from trane.core.labeler import *  # noqa
from trane.core.prediction_problem_saver import *  # noqa
from trane.ops.aggregation_ops import *  # noqa
from trane.ops.filter_ops import *  # noqa
from trane.ops.row_ops import *  # noqa
from trane.ops.transformation_ops import *  # noqa

"""TESTING STRATEGY:
Function: execute()
1. Ensure that a prediction problem is applied correctly.
2. Ensure output dimensions are the correct shapes.
3. Ensure all other values are un-changed (entity_id and cutoff_time)
"""

meta_json_str = '{ "path": "", "tables": [ { "path": "synthetic_taxi_data.csv",\
    "name": "taxi_data", "fields": [ {"name": "vendor_id", "type": "id"},\
    {"name": "taxi_id", "type": "id"}, {"name": "trip_id", "type": "datetime"}, \
    {"name": "distance", "type": "number", "subtype": "float"}, \
    {"name": "duration", "type": "number", "subtype": "float"}, \
    {"name": "fare", "type": "number", "subtype": "float"}, \
    {"name": "num_passengers", "type": "number", "subtype": "float"} ] } ]}'

dataframe = pd.DataFrame([
    (0, 0, 0, 5.32, 19.7, 53.89, 1, np.datetime64('1990-12-31')),
    (0, 0, 1, 1.08, 6.78, 18.89, 2, np.datetime64('1995-12-31')),
    (0, 0, 2, 4.69, 4.11, 41.35, 4, np.datetime64('2000-12-31'))],
    columns=["vendor_id", "taxi_id", "trip_id", "distance",
             "duration", "fare", "num_passengers",
             "date"])
