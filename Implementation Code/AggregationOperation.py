from Operation import Operation
import pandas as pd
from SubOperation import SubOperation
class AggregationOperation(Operation):
	
	#MULTIROW OPERATION TO BE APPLIED TO ALL COLUMNS FOR ALL ENTITIES
	#AGGREGATES ALL ROWS INTO A SINGLE ROW

	#Aggregation operation functions operate on the entire dataframe
	def last(dataframe):
		df = dataframe.copy()
		return df.tail(n = 1)
	def first(dataframe):
		df = dataframe.copy()
		return df.head(n = 1)
	def last_minus_first(dataframe):
		df = dataframe.copy()
		last = last(df)
		first = first(df)
		return last - first

	possible_operations = {
		"last" : last,
		"first": first,
		"last minus first" : last_minus_first
	}

	"""
	Args:
		Takes as input a single aggregation operation to perform from the dictionary
		of possible aggregation operations defined above. (TODO Find a good way 
		to define the dictionaries location)
	Returns:
		None
	Raises:
	    None
	"""
	def __init__(self, aggregation_operation_name):
		self.aggregation_operation = AggregationOperation.possible_operations[aggregation_operation_name]

	def execute(self, dataset):
		return self.aggregation_operation(dataset)

	def __str__(self):
		return "Aggregation operation (" + self.aggregation_operation_name + ")"

#SMALL TEST
# df = pd.DataFrame([[74, 200, 22, "Alex"],[71, 140, 19, "Shea"], [75, 170, 20, "Abby"]], columns = ['height', 'weight', 'age', 'name'])
# df.loc[3] = {"height":78, "name":"Future Alex", "weight":105, "age":25}
# agg_op = AggregationOperation("first")
# print agg_op.execute(df)
