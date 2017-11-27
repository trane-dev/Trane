import pandas as pd
from PredictionProblem import PredictionProblem
from AggregationOperation import AggregationOperation
from RowOperation import RowOperation
from TransformationOperation import TransformationOperation
from FilterOperation import FilterOperation

import AggregationOperationModule as ag
import RowOperationModule as ro
import TransformationOperationModule as tr
import FilterOperationModule as fi
class PredictionProblemGenerator:

	"""
	Args:
		(String) label_generating_column: the column of interest. This column
			will be solely used for performing operations against.
		(String) entity_id_column: the column with entity id's.
		(String) time_column: the name of the column containing time information.
	Returns:
		None
	"""
	def __init__(self, label_generating_column, entity_id_column, time_column):
		self.label_generating_column = label_generating_column
		self.entity_id_column = entity_id_column
		self.time_column = time_column
	"""
	Inspired by Ben Shreck's MIT MEng Thesis pg. 57, named LittleTrane.
		LittleTrane imposes constraints on PredictionProblem definitions.
		Beginning with a simple implementation.
		FilterOp - RowOp - TransOp - AggOp
	We may expand the possibilities later, but for now we start simple.
	Args:
		None
	Returns:
		(List): A list of prediction problems.
	"""
	#possible_operations for each class is a dictionary mapping a string name to an Operation of that class.
	def generate(self):
		possible_row_operation_names = ro.possible_operations.keys()
		possible_filter_operation_names = fi.possible_operations.keys()

		possible_aggregation_operation_names = ag.possible_operations.keys()
		possible_transformation_operation_names = tr.possible_operations.keys()

		column_to_operate_over = self.label_generating_column
		prediction_problems = []

		for aggregation_operation_name in possible_aggregation_operation_names:
			for transformation_operation_name in possible_transformation_operation_names:
				for row_operation_name in possible_row_operation_names:
					for filter_operation_name in possible_filter_operation_names:
						# for column_to_operate_over in column_names: #TODO DECIDE HOW TO ITERATE OVER COLUMN NAMES

						aggregation_operation = AggregationOperation(aggregation_operation_name)
						transformation_operation = TransformationOperation(column_to_operate_over, transformation_operation_name)
						row_operation = RowOperation(column_to_operate_over, row_operation_name)
						filter_operation = FilterOperation(column_to_operate_over, filter_operation_name)

						prediction_problem = PredictionProblem([filter_operation, row_operation, transformation_operation, aggregation_operation],
							self.label_generating_column, self.entity_id_column, self.time_column)

						prediction_problems.append(prediction_problem)

		return prediction_problems

if __name__ == '__main__':
	df = pd.read_csv('../../test_datasets/synthetic_taxi_data.csv')
	gen = PredictionProblemGenerator(df, "fare", "taxi_id", "time")
	pred_problems = gen.generate()
	print([str(pred_problem) for pred_problem in pred_problems][0])
	# print(pred_problems[25])
	# print(pred_problems[25].execute(df))
	# print(len([str(pred_problem) for pred_problem in pred_problems]))
