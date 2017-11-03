import pandas as pd
from PredictionProblem import PredictionProblem
from AggregationOperation import AggregationOperation
from RowOperation import RowOperation
from TransformationOperation import TransformationOperation
from FilterOperation import FilterOperation

class PredictionProblemGenerator:

	#OPERATION TYPE:
	#POSSIBLE OPERATIONS WITHIN THAT CLASS
	#E.G. ROW OPERATION: IDENTITY, GREATER THAN, 
	#	AGGREGATION OPERATION: LAST, FIRST
	#	TRANSFORMATION OPERATION : DIFF

	


	def __init__(self, dataframe):
		self.dataframe = dataframe

	#Inspired by Ben Shreck's MIT MEng Thesis pg. 57, named LittleTrane.
	#	LittleTrane imposes constraints on PredictionProblem definitions.
	#	Beginning with a simple implementation.
	#	FilterOp - RowOp - TransOp - AggOp
	#We may expand the possibilities later, but for now we start simple.

	#possible_operations for each class is a dictionary mapping a string name to an Operation of that class.

	def generate(self):
		possible_row_operation_names = RowOperation.possible_operations.keys()
		possible_filter_operation_names = FilterOperation.possible_operations.keys()
		
		possible_aggregation_operation_names = AggregationOperation.possible_operations.keys()
		possible_transformation_operation_names = TransformationOperation.possible_operations.keys()

		# column_names = list(self.dataframe)
		column_to_operate_over = "height"
		prediction_problems = []

		for aggregation_operation_name in possible_aggregation_operation_names:
			for transformation_operation_name in possible_transformation_operation_names:
				for row_operation_name in possible_row_operation_names:
					for filter_operation_name in possible_filter_operation_names:
						# for column_to_operate_over in column_names: #TODO DECIDE HOW TO ITERATE OVER COLUMN NAMES
							
						aggregation_operation = AggregationOperation(aggregation_operation_name)
						transformation_operation = TransformationOperation(transformation_operation_name)
						
						row_operation = RowOperation(column_to_operate_over, row_operation_name)
						filter_operation = FilterOperation(column_to_operate_over, filter_operation_name)
	
						prediction_problem = PredictionProblem([filter_operation, row_operation, transformation_operation, aggregation_operation])
						prediction_problems.append(prediction_problem)

		return prediction_problems


df = pd.DataFrame([[74, 200, 22, "Alex"],[71, 140, 19, "Shea"], [75, 170, 20, "Abby"]], columns = ['height', 'weight', 'age', 'name'])
df.loc[3] = {"height":78, "name":"Future Alex", "weight":105, "age":25}
gen = PredictionProblemGenerator(df)
pred_problems = gen.generate()
print len([str(pred_problem) for pred_problem in pred_problems])