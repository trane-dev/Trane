import pandas as pd
from RowOperation import RowOperation
from AggregationOperation import AggregationOperation
class PredictionProblem:

	"""
	Prediction Problem is made up of a list of Operations. The list of operations delineate
	the order the operations will be applied in. 
	"""
	
	"""
	Args:
		(List) Operations: a list of operations (class Operation) that define the
			order in which operations should take place.
	Returns:
		None
	"""
	def __init__(self, operations):
		self.operations = operations

	"""
	This function executes all the operations on the dataframe and returns the output. The output
	should be structured as a single label/value per the Trane documentation.
	See paper: "What would a data scientist ask? Automatically formulating and solving predicton
	problems."
	Args:
		(Pandas DataFrame): the dataframe containing the data we wish to analyze.
	Returns:
		(Boolean/Float): The Label/Value of the prediction problem's formulation when applied to the data.
	"""
	def execute(self, dataframe):
		output = dataframe.copy()
		for operation in self.operations:
			print "output: \n" + str(output)
			output = operation.execute(output)
		return output
	"""
	Args:
		None
	Returns:
		A natural language text describing the prediction problem. 
	"""
	def __str__(self):
		description = ""
		last_op_idx = len(self.operations) - 1
		for idx, operation in enumerate(self.operations):
			description += str(operation)
			if idx != last_op_idx:
				description += "->"
		return description

	"""
	This function generates the cutoff times for each entity id.
	Args:
		(Pandas DataFrame): the dataframe containing the data we wish to analyze.)
	Returns:
		(Dict): Entity Id to Cutoff time mapping. 
	"""
	def determine_cutoff_time(self, dataframe):
		initial_time = 
		end_time = 
		cutoff = to be determined. some interesting tunable combination using end_time and initial_time
		return a entity id cutoff_time mapping




#SMALL TEST
# df = pd.DataFrame([[74, 200, 22, "Alex"],[71, 140, 19, "Shea"], [75, 170, 20, "Abby"]], columns = ['height', 'weight', 'age', 'name'])
# df.loc[3] = {"height":78, "name":"Future Alex", "weight":105, "age":25}
# row_op = RowOperation("age", "identity")
# agg_op = AggregationOperation("first")
# pred_problem = PredictionProblem([row_op, agg_op])
# print pred_problem.execute(df)
