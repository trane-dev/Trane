import sys
sys.path.insert(0, '/Users/Alexander/Documents/Trane__HDI_REPO/')
import trane
from trane.core.prediction_problem import PredictionProblem
from trane.ops.row_ops import *
from trane.ops.filter_ops import *
from trane.ops.transformation_ops import *
from trane.ops.aggregation_ops import *
from trane.utils.table_meta import TableMeta
from trane.core.prediction_problem_saver import *
import os

"""TESTING STRATEGY:
1. Write then read prediction problems and other meta info to json and 
	ensure the information is preserved.
"""
json_str = '{ "path": "", "tables": [ { "path": "synthetic_taxi_data.csv", "name": "taxi_data", "fields": [ {"name": "vendor_id", "type": "id"}, {"name": "taxi_id", "type": "id"}, {"name": "trip_id", "type": "datetime"}, {"name": "distance", "type": "number", "subtype": "float"}, {"name": "duration", "type": "number", "subtype": "float"}, {"name": "fare", "type": "number", "subtype": "float"}, {"name": "num_passengers", "type": "number", "subtype": "float"} ] } ]}'


def test_write_then_read():
    table_meta = TableMeta.from_json(json_str)
    entity_id_column = "taxi_id"
    time_column = "trip_id"
    label_generating_column = "fare"
    prediction_problem = PredictionProblem([AllFilterOp(label_generating_column),
                                            IdentityRowOp(
                                                label_generating_column),
                                            IdentityTransformationOp(
                                                label_generating_column),
                                            LastAggregationOp(label_generating_column)])
    filename = "prediction_problem.json"

    prediction_problems_to_json_file([prediction_problem], table_meta, entity_id_column,
                                     label_generating_column, time_column, filename)

    prediction_problems_from_json, table_meta_from_json, entity_id_column_from_json, \
        label_generating_column_from_json, time_column_from_json = prediction_problems_from_json_file(
            filename)
    prediction_problem_from_json = prediction_problems_from_json[0]

    os.remove(filename)

    assert(prediction_problem == prediction_problem_from_json)
    assert(entity_id_column == entity_id_column_from_json)
    assert(time_column == time_column_from_json)
    assert(label_generating_column == label_generating_column_from_json)
    assert(table_meta == table_meta_from_json)
