import pandas as pd
# from RowOperation import RowOperation
# from AggregationOperation import AggregationOperation
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
	def __init__(self, Operations):
		self.Operations = Operations

	def execute(self, dataframe):
		output = dataframe.copy()
		for operation in self.Operations:
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
		return ''.join(["" + str(operation) for operation in self.Operations])

#SMALL TEST
# df = pd.DataFrame([[74, 200, 22, "Alex"],[71, 140, 19, "Shea"], [75, 170, 20, "Abby"]], columns = ['height', 'weight', 'age', 'name'])
# df.loc[3] = {"height":78, "name":"Future Alex", "weight":105, "age":25}
# row_op = RowOperation({"age":"Identity", "name":"Identity"})
# agg_op = AggregationOperation(AggregationOperation.possible_ag_opps["FIRST"])
# pred_problem = PredictionProblem([row_op, agg_op])
# print pred_problem.execute(df)
