"""TESTING STRATEGY
Function: generate(self)
1. Ensure the correct # of problems are generated.
2. Generated problems are of proper type
3. Ensure prediction problems are generated in order of Filter->Row->Transformation->Aggregation
"""

from trane.core.prediction_problem import *
from trane.core.prediction_problem_generator import *
from trane.utils.table_meta import TableMeta
from trane.ops.row_ops import *
from trane.ops.filter_ops import *
from trane.ops.transformation_ops import *
from trane.ops.aggregation_ops import *
import pandas as pd
import logging
meta_json_str = '{ "path": "", "tables": [ { "path": "synthetic_taxi_data.csv", "name": "taxi_data", "fields": [ {"name": "vendor_id", "type": "id"}, {"name": "taxi_id", "type": "id"}, {"name": "trip_id", "type": "datetime"}, {"name": "distance", "type": "number", "subtype": "float"}, {"name": "duration", "type": "number", "subtype": "float"}, {"name": "fare", "type": "number", "subtype": "float"}, {"name": "num_passengers", "type": "number", "subtype": "float"} ] } ]}'
dataframe = pd.DataFrame([(0, 0, 0, 5.32, 19.7, 53.89, 1),
                          (0, 0, 1, 1.08, 6.78, 18.89, 2),
                          (0, 0, 2, 4.69, 14.11, 41.35, 4)],
                         columns=["vendor_id", "taxi_id", "trip_id", "distance", "duration", "fare", "num_passengers"])

def test_number_of_problems_generated():
    table_meta = TableMeta.from_json(meta_json_str)
    entity_id_column = "taxi_id"
    label_generating_column = "fare"
    time_column = "trip_id"
    filter_column = "taxi_id"
    ppg = PredictionProblemGenerator(
        table_meta, entity_id_column, label_generating_column, time_column, filter_column)
    generator = ppg.generator(dataframe)

    expected = 66  # THIS NUMBER WILL CHANGE IF MORE OPERATIONS ARE ADDED OR DECREASE BASED ON TYPE CHECKING
    found = len(list(generator))
    assert(expected == found)


def test_generated_types():
    table_meta = TableMeta.from_json(meta_json_str)
    entity_id_column = "taxi_id"
    label_generating_column = "fare"
    time_column = "trip_id"
    filter_column = "taxi_id"
    ppg = PredictionProblemGenerator(
        table_meta, entity_id_column, label_generating_column, time_column, filter_column)
    generator = ppg.generator(dataframe)

    expected = PredictionProblem
    problems = [prob for prob in generator]

    for problem in problems:
        found = problem
        assert(type(found) is expected)


def test_order_of_operations():
    table_meta = TableMeta.from_json(meta_json_str)
    entity_id_column = "taxi_id"
    label_generating_column = "fare"
    time_column = "trip_id"
    filter_column = "taxi_id"
    ppg = PredictionProblemGenerator(
        table_meta, entity_id_column, label_generating_column, time_column, filter_column)
    
    logging.debug("Dataframe in test_prediction_problem_generator.py: \n{}\n".format(dataframe))
    generator = ppg.generator(dataframe)

    problems = [prob for prob in generator]

    for problem in problems:
        ops = problem.operations
        assert(len(ops) == 4)
        first_op = ops[0]
        second_op = ops[1]
        third_op = ops[2]
        fourth_op = ops[3]

        assert(issubclass(first_op.__class__, FilterOpBase))
        assert(issubclass(second_op.__class__, RowOpBase))
        assert(issubclass(third_op.__class__, TransformationOpBase))
        assert(issubclass(fourth_op.__class__, AggregationOpBase))
