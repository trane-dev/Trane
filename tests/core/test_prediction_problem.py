import trane
from trane.core.prediction_problem import *
from trane.core.prediction_problem import entropy_of_a_list
from trane.ops.row_ops import *
from trane.ops.filter_ops import *
from trane.ops.transformation_ops import *
from trane.ops.aggregation_ops import *
from trane.utils.table_meta import TableMeta
import pandas as pd

dataframe = pd.DataFrame([(0, 0, 0, 5.32, 19.7, 53.89, 1),
						  (0, 0, 1, 1.08, 6.78, 18.89, 2),
						  (0, 0, 2, 4.69, 14.11, 41.35, 4)],
						 columns=["vendor_id", "taxi_id", "trip_id", "distance", "duration", "fare", "num_passengers"])

json_str = '{ "path": "", "tables": [ { "path": "synthetic_taxi_data.csv", "name": "taxi_data", "fields": [ {"name": "vendor_id", "type": "id"}, {"name": "taxi_id", "type": "id"}, {"name": "trip_id", "type": "datetime"}, {"name": "distance", "type": "number", "subtype": "float"}, {"name": "duration", "type": "number", "subtype": "float"}, {"name": "fare", "type": "number", "subtype": "float"}, {"name": "num_passengers", "type": "number", "subtype": "float"} ] } ]}'

dataframe2 = pd.DataFrame([(0, 0),
							(1, 1),
							(2, 2),
							(3, 3),
							(4, 4),
							(5, 5),
							(6, 6),
							(7, 7),
							(8, 8),
							(9, 9),
							(10, 10)], columns = ['c1', 'c2'])

def test_hyper_parameter_generation():
	label_generating_column = "fare"
	filter_column = "taxi_id"
	prediction_problem = PredictionProblem([GreaterFilterOp(filter_column),
														  GreaterRowOp(
															  label_generating_column),
														  IdentityTransformationOp(
															  label_generating_column),
														  LastAggregationOp(label_generating_column)])
	prediction_problem.generate_and_set_hyper_parameters(dataframe, 
									label_generating_column, filter_column, {})

	op1_hyper_parameter = prediction_problem.operations[0].hyper_parameter_settings
	op2_hyper_parameter = prediction_problem.operations[1].hyper_parameter_settings
	op3_hyper_parameter = prediction_problem.operations[2].hyper_parameter_settings
	op4_hyper_parameter = prediction_problem.operations[3].hyper_parameter_settings

	assert(op1_hyper_parameter['threshold'] == 0)
	assert(op2_hyper_parameter['threshold'] == 41.35)

def test_hyper_parameter_generation_hashing():
	label_generating_column = "fare"
	filter_column = "taxi_id"
	ops = [GreaterFilterOp(filter_column),
	GreaterRowOp(label_generating_column),
	IdentityTransformationOp(label_generating_column),
	LastAggregationOp(label_generating_column)]

	prediction_problem = PredictionProblem(ops)
	hyper_parameter_settings_memo_table = {}
	hyper_parameter_settings_memo_table[hash(ops[0])] = 20
	hyper_parameter_settings_memo_table[hash(ops[1])] = 61.35

	prediction_problem.generate_and_set_hyper_parameters(dataframe, 
									label_generating_column, filter_column,
									hyper_parameter_settings_memo_table)

	op1_hyper_parameter = prediction_problem.operations[0].hyper_parameter_settings
	op2_hyper_parameter = prediction_problem.operations[1].hyper_parameter_settings
	op3_hyper_parameter = prediction_problem.operations[2].hyper_parameter_settings
	op4_hyper_parameter = prediction_problem.operations[3].hyper_parameter_settings

	assert(op1_hyper_parameter['threshold'] == 20)
	assert(op2_hyper_parameter['threshold'] == 61.35)

def test_hashing_collisions():
	col1 = "a"
	col2 = "b"
	op1 = GreaterFilterOp(col1)
	op2 = GreaterFilterOp(col2)
	op3 = GreaterFilterOp(col1)

	op1_hash = hash(op1)
	op2_hash = hash(op2)
	op3_hash = hash(op3)

	assert(op1_hash != op2_hash)
	assert(op1_hash == op3_hash)

def test_hyper_parameter_generation_2():
	label_generating_column = "c1"
	filter_column = "c2"
	
	prediction_problem = PredictionProblem([GreaterFilterOp(filter_column),
														  GreaterRowOp(
															  label_generating_column),
														  IdentityTransformationOp(
															  label_generating_column),
														  LastAggregationOp(label_generating_column)])
	prediction_problem.generate_and_set_hyper_parameters(dataframe2, 
									label_generating_column, filter_column, {})

	op1_hyper_parameter = prediction_problem.operations[0].hyper_parameter_settings
	op2_hyper_parameter = prediction_problem.operations[1].hyper_parameter_settings
	op3_hyper_parameter = prediction_problem.operations[2].hyper_parameter_settings
	op4_hyper_parameter = prediction_problem.operations[3].hyper_parameter_settings

	print(op1_hyper_parameter)
	print(op2_hyper_parameter)
	assert(op1_hyper_parameter['threshold'] == 1)
	assert(op2_hyper_parameter['threshold'] == 4)   

def test_op_type_check():
	filter_column = "fare"
	label_generating_column = "fare"
	table_meta = TableMeta.from_json(json_str)

	prediction_problem_correct_types = PredictionProblem([AllFilterOp(filter_column),
														  IdentityRowOp(
															  label_generating_column),
														  IdentityTransformationOp(
															  label_generating_column),
														  LastAggregationOp(label_generating_column)])

	label_generating_column = "vendor_id"
	prediction_problem_incorrect_types = PredictionProblem([AllFilterOp(filter_column),
															IdentityRowOp(
																label_generating_column),
															IdentityTransformationOp(
																label_generating_column),
															LMFAggregationOp(label_generating_column)])

	(correct_is_valid, filter_column_types_A, label_generating_column_types_A) = prediction_problem_correct_types.is_valid_prediction_problem(table_meta, filter_column, "fare")
	(incorrect_is_valid, filter_column_types_B, label_generating_column_types_B) = prediction_problem_incorrect_types.is_valid_prediction_problem(table_meta, filter_column, label_generating_column)

	assert(filter_column_types_A == ['float', 'float'])
	assert(label_generating_column_types_A == ['float', 'float', 'float', 'float'])
	
	assert(filter_column_types_B == None)
	assert(label_generating_column_types_B == None)

	assert(correct_is_valid)
	assert(not incorrect_is_valid)


def test_execute():
	label_generating_column = "fare"
	multiple_csv = ["test_data/synthetic_taxi_data.csv"]
	df = dataframe

	time_column = "trip_id"
	cutoff_time = 100

	prediction_problem = PredictionProblem([AllFilterOp(label_generating_column),
											IdentityRowOp(
												label_generating_column),
											IdentityTransformationOp(
												label_generating_column),
											LastAggregationOp(label_generating_column)])


	expected = 41.35
	precutoff_time, all_data = prediction_problem.execute(
		df, time_column, cutoff_time, ['float', 'float'], ['float', 'float', 'float', 'float'])
		
	found = precutoff_time[label_generating_column].iloc[0]
	assert(expected == found)


def test_to_and_from_json():
	label_generating_column = "fare"
	prediction_problem = PredictionProblem([AllFilterOp(label_generating_column),
											IdentityRowOp(
												label_generating_column),
											IdentityTransformationOp(
												label_generating_column),
											LastAggregationOp(label_generating_column)])
	json_str = prediction_problem.to_json()
	prediction_problem_from_json = PredictionProblem.from_json(json_str)

	assert(prediction_problem == prediction_problem_from_json)

def test_to_and_from_json_with_order_of_types():
	label_generating_column = "fare"
	prediction_problem = PredictionProblem([AllFilterOp(label_generating_column),
											IdentityRowOp(
												label_generating_column),
											IdentityTransformationOp(
												label_generating_column),
											LastAggregationOp(label_generating_column)])
	prediction_problem.filter_column_order_of_types = [TableMeta.TYPE_INTEGER]
	prediction_problem.label_generating_column_order_of_types = \
		[TableMeta.TYPE_INTEGER, TableMeta.TYPE_BOOL, TableMeta.TYPE_CATEGORY]
	json_str = prediction_problem.to_json()
	prediction_problem_from_json = PredictionProblem.from_json(json_str)

	assert(prediction_problem == prediction_problem_from_json)

def test_equality():
	label_generating_column = "fare"
	prediction_problem = PredictionProblem([AllFilterOp(label_generating_column),
											IdentityRowOp(
												label_generating_column),
											IdentityTransformationOp(
												label_generating_column),
											LastAggregationOp(label_generating_column)])
	prediction_problem_clone = PredictionProblem([AllFilterOp(label_generating_column),
												  IdentityRowOp(
													  label_generating_column),
												  IdentityTransformationOp(
													  label_generating_column),
												  LastAggregationOp(label_generating_column)])

	assert(prediction_problem_clone == prediction_problem)

def test_entropy():
	a = [1,2,3,4,5,6,7,8]
	entropy_a = entropy_of_a_list(a)
	b = [1,1,2,3,4,5,6,7]
	entropy_b = entropy_of_a_list(b)
	c = [1,1,1,1,1,1,1,1]
	entropy_c = entropy_of_a_list(c)
	d = [1,2]
	entropy_d = entropy_of_a_list(d)
	
	assert(entropy_a > entropy_b > entropy_d > entropy_c)
	assert(entropy_a == 2.0794415416798357)
	assert(entropy_b == 1.9061547465398496)
	assert(entropy_c == 0.0)
	assert(entropy_d == 0.6931471805599453)










