"""TESTING STRATEGY
Function: generate(self)
1. Ensure the correct # of problems are generated.
2. Generated problems are of proper type
3. Ensure prediction problems are generated in order of Filter->Row->Transformation->Aggregation
"""

import pandas as pd

from trane.core.prediction_problem import *  # noqa
from trane.core.prediction_problem_generator import *  # noqa
from trane.ops.aggregation_ops import *  # noqa
from trane.ops.filter_ops import *  # noqa
from trane.ops.row_ops import *  # noqa
from trane.ops.transformation_ops import *  # noqa

meta_json_str = '{ "path": "", "tables": [ { "path": "synthetic_taxi_data.csv",\
    "name": "taxi_data", "fields": [ {"name": "vendor_id", "type": "id"},\
    {"name": "taxi_id", "type": "id"}, {"name": "trip_id", "type": "datetime"}, \
    {"name": "distance", "type": "number", "subtype": "float"}, \
    {"name": "duration", "type": "number", "subtype": "float"}, \
    {"name": "fare", "type": "number", "subtype": "float"}, \
    {"name": "num_passengers", "type": "number", "subtype": "float"} ] } ]}'
dataframe = pd.DataFrame([(0, 0, 0, 5.32, 19.7, 53.89, 1),
                          (0, 0, 1, 1.08, 6.78, 18.89, 2),
                          (0, 0, 2, 4.69, 14.11, 41.35, 4)],
                         columns=["vendor_id", "taxi_id", "trip_id", "distance",
                                  "duration", "fare", "num_passengers"])
