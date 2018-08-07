import os

import numpy as np
import pandas as pd

import trane
from trane.core.labeler import *  # noqa
from trane.core.prediction_problem import PredictionProblem
from trane.core.prediction_problem_saver import *  # noqa
from trane.ops.aggregation_ops import *  # noqa
from trane.ops.filter_ops import *  # noqa
from trane.ops.row_ops import *  # noqa
from trane.ops.transformation_ops import *  # noqa
from trane.utils.table_meta import TableMeta

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


def test_labeler_apply():
    entity_id_column = "taxi_id"
    time_column = "date"
    entity_id_column = 'trip_id'
    label_generating_column = "fare"
    filter_column = "fare"
    table_meta = TableMeta.from_json(meta_json_str)
    labeler = Labeler()

    # test_data = pd.DataFrame(
    #     data=np.arange(90).reshape(-1, 3),
    #     columns=['entity_id', 'other_data', 'other_data_2'])

    # import pdb; pdb.set_trace()
    cutoff_df = trane.CutoffStrategy(
        lambda x, y: (
            np.datetime64('1980-02-25'), np.datetime64('2000-02-25')),
        'description').generate_cutoffs(dataframe, entity_id_column)

    print(cutoff_df)

    operations = [
        AllFilterOp(label_generating_column),
        IdentityRowOp(label_generating_column),
        IdentityTransformationOp(label_generating_column),
        LastAggregationOp(label_generating_column)]

    prediction_problem = PredictionProblem(
        operations=operations,
        cutoff_strategy=None)

    (is_valid_prediction_problem,
        filter_column_order_of_types,
        label_generating_column_order_of_types)\
        = prediction_problem.is_valid_prediction_problem(
            table_meta, filter_column, label_generating_column)

    filename = "prediction_problem.json"

    # import pdb
    # pdb.set_trace()
    prediction_problems_to_json_file(
        [prediction_problem], table_meta, entity_id_column,
        label_generating_column, time_column, filename)

    labeler.execute(
        data=dataframe,
        cutoff_df=cutoff_df,
        json_prediction_problems_filename=filename)
    os.remove(filename)
