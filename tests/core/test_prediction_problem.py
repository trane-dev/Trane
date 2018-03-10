import sys
sys.path.insert(0, '/Users/Alexander/Documents/Trane__HDI_REPO/')
import trane
from trane.core.prediction_problem import PredictionProblem
from trane.ops.row_ops import *
from trane.ops.filter_ops import *
from trane.ops.transformation_ops import *
from trane.ops.aggregation_ops import *
from trane.utils.table_meta import TableMeta
import pandas as pd
"""TESTING STRATEGY:
Function: op_type_check()

Function: execute()

Function: to_json and from_json()

"""
dataframe = pd.DataFrame([(0, 0, 0, 5.32, 19.7, 53.89, 1), 
							(0, 0, 1, 1.08, 6.78, 18.89, 2),
							(0, 0, 2, 4.69, 14.11, 41.35, 4)], 
							columns = ["vendor_id", "taxi_id", "trip_id", "distance", "duration", "fare", "num_passengers"])

json_str = '{ "path": "", "tables": [ { "path": "synthetic_taxi_data.csv", "name": "taxi_data", "fields": [ {"name": "vendor_id", "type": "id"}, {"name": "taxi_id", "type": "id"}, {"name": "trip_id", "type": "datetime"}, {"name": "distance", "type": "number", "subtype": "float"}, {"name": "duration", "type": "number", "subtype": "float"}, {"name": "fare", "type": "number", "subtype": "float"}, {"name": "num_passengers", "type": "number", "subtype": "float"} ] } ]}'
def test_op_type_check():
	label_generating_column = "fare"
	table_meta = TableMeta.from_json(json_str)

	prediction_problem_correct_types = PredictionProblem([AllFilterOp(label_generating_column),
											IdentityRowOp(label_generating_column),
											IdentityTransformationOp(label_generating_column),
											LastAggregationOp(label_generating_column)])

	label_generating_column = "vendor_id"
	prediction_problem_incorrect_types = PredictionProblem([AllFilterOp(label_generating_column),
											IdentityRowOp(label_generating_column),
											IdentityTransformationOp(label_generating_column),
											LMFAggregationOp(label_generating_column)])
	
	assert(prediction_problem_correct_types.op_type_check(table_meta))
	assert(not prediction_problem_incorrect_types.op_type_check(table_meta))

def test_execute():
	label_generating_column = "fare"
	multiple_csv = ["test_data/synthetic_taxi_data.csv"]
	df = dataframe

	time_column = "trip_id"
	cutoff_time = 0

	prediction_problem = PredictionProblem([AllFilterOp(label_generating_column),
											IdentityRowOp(label_generating_column),
											IdentityTransformationOp(label_generating_column),
											LastAggregationOp(label_generating_column)])

	expected = 41.35
	precutoff_time, all_data = prediction_problem.execute(df, time_column, cutoff_time)
	found = precutoff_time[label_generating_column].iloc[0]
	assert(expected == found)

def test_to_and_from_json():
	label_generating_column = "fare"
	prediction_problem = PredictionProblem([AllFilterOp(label_generating_column),
											IdentityRowOp(label_generating_column),
											IdentityTransformationOp(label_generating_column),
											LastAggregationOp(label_generating_column)])
	json_str = prediction_problem.to_json()
	prediction_problem_from_json = PredictionProblem.from_json(json_str)

	assert(prediction_problem == prediction_problem_from_json)

def test_equality():
	label_generating_column = "fare"
	prediction_problem = PredictionProblem([AllFilterOp(label_generating_column),
											IdentityRowOp(label_generating_column),
											IdentityTransformationOp(label_generating_column),
											LastAggregationOp(label_generating_column)])
	prediction_problem_clone = PredictionProblem([AllFilterOp(label_generating_column),
											IdentityRowOp(label_generating_column),
											IdentityTransformationOp(label_generating_column),
											LastAggregationOp(label_generating_column)])

	assert(prediction_problem_clone == prediction_problem)

test_op_type_check()







