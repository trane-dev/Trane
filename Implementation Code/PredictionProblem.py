import pandas as pd
from RowOperation import RowOperation
from AggregationOperation import AggregationOperation
class PredictionProblem:

	"""
	Args:
		(List) Operations: a list of operations (class Operation) that define the
			order in which operations should take place.
	Returns:
		None
	Raises:
	    None
	"""
	def __init__(self, operations):
		self.operations = operations

	def execute(self, dataframe):
		output = dataframe.copy()
		for operation in self.operations:
			print "output: \n" + str(output)
			output = operation.execute(output)
		return output
	"""
	Args:

	Returns:
		A natural language text describing the prediction problem. 
	Raises:
	    None
	"""
	def __str__(self):
		description = ""
		last_op_idx = len(self.operations) - 1
		for idx, operation in enumerate(self.operations):
			description += str(operation)
			if idx != last_op_idx:
				description += "->"
		return description

#SMALL TEST
# df = pd.DataFrame([[74, 200, 22, "Alex"],[71, 140, 19, "Shea"], [75, 170, 20, "Abby"]], columns = ['height', 'weight', 'age', 'name'])
# df.loc[3] = {"height":78, "name":"Future Alex", "weight":105, "age":25}
# row_op = RowOperation("age", "identity")
# agg_op = AggregationOperation("first")
# pred_problem = PredictionProblem([row_op, agg_op])
# print pred_problem.execute(df)
