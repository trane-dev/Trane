from Operation import Operation
import pandas as pd
import AggregationOperationModule as ag
class AggregationOperation(Operation):
	"""
	Aggregation Operations are operations that take many rows and aggregate them
	into a single value.
	Transformation operations are similar to Aggregation operations, except that transformation operations must
	return a dataframe with more than 2 rows and an aggregation operaton must return a single row.
	"""

	"""
	Args:
		Takes as input a single aggregation operation to perform from the dictionary
		of possible aggregation operations defined above.
	Returns:
		None
	"""
	def __init__(self, aggregation_operation_name):
		self.aggregation_operation_name = aggregation_operation_name
		self.aggregation_operation = ag.possible_operations[aggregation_operation_name]

	def execute(self, dataset):
		return self.aggregation_operation(dataset)

	def __str__(self):
		return "Aggregation operation (" + self.aggregation_operation_name + ")"

#SMALL TEST
# df = pd.DataFrame([[74, 200, 22, "Alex"],[71, 140, 19, "Shea"], [75, 170, 20, "Abby"]], columns = ['height', 'weight', 'age', 'name'])
# df.loc[3] = {"height":78, "name":"Future Alex", "weight":105, "age":25}
# agg_op = AggregationOperation("last minus first")
# print(agg_op.execute(df))
