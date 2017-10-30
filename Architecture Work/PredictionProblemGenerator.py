import pandas as pd
from PredictionProblem import PredictionProblem
from AggregationOperation import AggregationOperation
from RowOperation import RowOperation
from TransformationOperation import TransformationOperation
from MultiRowOperation import MultiRowOperation

class PredictionProblemGenerator:

	#OPERATION TYPE:
	#POSSIBLE OPERATIONS WITHIN THAT CLASS
	#E.G. ROW OPERATION: IDENTITY, GREATER THAN, 
	#	AGGREGATION OPERATION: LAST, FIRST
	#	TRANSFORMATION OPERATION : DIFF

	possible_row_operations = RowOperation.possible_operations
	possible_aggregation_operations = AggregationOperation.possible_operations
	possible_transformation_operations = TransformationOperation.possible_operations
	possible_filter_operations = FilterOperation.possible_operations


	def __init__(self, dataframe):
		self.dataframe = dataframe

	#Inspired by Ben Shreck's MIT MEng Thesis pg. 57, named LittleTrane.
	#	LittleTrane imposes constraints on PredictionProblem definitions.
	#	Beginning with a simple implementation.
	#	FilterOp - RowOp - MultiRow Op
	#We may expand the possibilities later, but for now we start simple.
	def generate(self):
		all_possible_filter_ops = 
		all_possible_row_ops = 
		all_possible_multi_row_ops = 

		prediction_problems = []

		for filter_op in all_possible_filter_ops:
			for row_op in all_possible_row_ops:
				for multi_row_op in all_possible_multi_row_ops:
					prediction_problem = PredictionProblem([filter_op, row_op, multi_row_op])
					prediction_problems.append(prediction_problem)

		return prediction_problems