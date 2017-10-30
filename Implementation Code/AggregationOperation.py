from Operation import Operation
import pandas as pd
from SubOperation import SubOperation
class AggregationOperation(Operation):
	
	#MULTIROW OPERATION TO BE APPLIED TO ALL COLUMNS FOR ALL ENTITIES
	#AGGREGATES ALL ROWS INTO A SINGLE ROW

	def last(dataframe):
		return dataframe.tail(n = 1)
	def first(dataframe):
		return dataframe.head(n = 1)
	possible_operations = {
		"last" : last,
		"first": first
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
	def __init__(self, multi_row_operation):
		self.multi_row_operation = multi_row_operation

	def execute(self, dataset):
		return self.multi_row_operation(dataset)

	def __str__(self):
		return "Aggregation Operation"

#SMALL TEST
# df = pd.DataFrame([[74, 200, 22, "Alex"],[71, 140, 19, "Shea"], [75, 170, 20, "Abby"]], columns = ['height', 'weight', 'age', 'name'])
# df.loc[3] = {"height":78, "name":"Future Alex", "weight":105, "age":25}
# agg_op = AggregationOperation(AggregationOperation.possible_ag_opps["FIRST"])
# print agg_op.execute(df)
