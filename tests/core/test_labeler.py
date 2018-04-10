# import sys
# sys.path.insert(0, '/Users/Alexander/Documents/Trane__HDI_REPO/')
from trane.core.labeler import *
import trane
from trane.core.prediction_problem import PredictionProblem
from trane.ops.row_ops import *
from trane.ops.filter_ops import *
from trane.ops.transformation_ops import *
from trane.ops.aggregation_ops import *
from trane.utils.table_meta import TableMeta
from trane.core.prediction_problem_saver import *
import json
import pandas as pd
import os

"""TESTING STRATEGY:
Function: execute()
1. Ensure that a prediction problem is applied correctly.
2. Ensure output dimensions are the correct shapes.
3. Ensure all other values are un-changed (entity_id and cutoff_time)
"""

meta_json_str = '{ "path": "", "tables": [ { "path": "synthetic_taxi_data.csv", "name": "taxi_data", "fields": [ {"name": "vendor_id", "type": "id"}, {"name": "taxi_id", "type": "id"}, {"name": "trip_id", "type": "datetime"}, {"name": "distance", "type": "number", "subtype": "float"}, {"name": "duration", "type": "number", "subtype": "float"}, {"name": "fare", "type": "number", "subtype": "float"}, {"name": "num_passengers", "type": "number", "subtype": "float"} ] } ]}'
dataframe = pd.DataFrame([(0, 0, 0, 5.32, 19.7, 53.89, 1),
                          (0, 0, 1, 1.08, 6.78, 18.89, 2),
                          (0, 0, 2, 4.69, 14.11, 41.35, 4)],
                         columns=["vendor_id", "taxi_id", "trip_id", "distance", "duration", "fare", "num_passengers"])


def test_labeler_apply():
    entity_id_column = "taxi_id"
    time_column = "trip_id"
    label_generating_column = "fare"
    filter_column = "fare"
    table_meta = TableMeta.from_json(meta_json_str)
    labeler = Labeler()
    df = dataframe
    entity_to_data_dict = trane.df_group_by_entity_id(df, entity_id_column)
    entity_id_to_data_and_cutoff_dict = trane.ConstantCutoffTime(
        0, 0).generate_cutoffs(entity_to_data_dict, time_column)

    prediction_problem = PredictionProblem([AllFilterOp(label_generating_column),
                                            IdentityRowOp(
                                                label_generating_column),
                                            IdentityTransformationOp(
                                                label_generating_column),
                                            LastAggregationOp(label_generating_column)])

    (is_valid_prediction_problem, filter_column_order_of_types, label_generating_column_order_of_types) = prediction_problem.is_valid_prediction_problem(
                table_meta, filter_column, label_generating_column)
    
    filename = "prediction_problem.json"

    prediction_problems_to_json_file(
        [prediction_problem], table_meta, entity_id_column, label_generating_column, time_column, filename)

    input_ = entity_id_to_data_and_cutoff_dict
    expected = pd.DataFrame([[0, 41.35, 0]], columns=[
                            entity_id_column, 'problem_label', 'cutoff_time'])
    found = labeler.execute(entity_id_to_data_and_cutoff_dict, filename)
    os.remove(filename)
