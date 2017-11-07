from Operation import Operation
import pandas as pd
from SubOperation import SubOperation

class RowOperation(Operation):
	

	"""
	A Row Operation takes in as input a dataset. The row operation then applies some value to all rows 
	for a specific column within the dataframe.
	"""

	"""
	Classwide methods that act as possible RowOperations.
	There are two steps involved in adding a new method.
	1. Create a new function.
	2. Create a new mapping in the dictionary with the function inside the SubOperation class.
	"""
	def identity(val):
		return val
	def equals(val, param):
		return val == param
	def not_equals(val, param):
		return val != param
	def less_than(val, param):
		return val < param
	def greater_than(val, param):
		return val > param
	def exponentiate(val, param):
		return val ** param

	param_placeholder = 1 #TODO UPDATE WITH A FUNCTION TO PROGRAMATICALLY
		#SPECIFY WHAT THE PARAMS FOR EACH FUNCTION WILL BE

	possible_operations = {
	"identity" : SubOperation("identity", identity, param_placeholder),
	"equals" : SubOperation("equals", equals, param_placeholder),
	"not equals" : SubOperation("not equals", not_equals, param_placeholder),
	"less than" : SubOperation("less than", less_than, param_placeholder),
	"greater than" : SubOperation("greater than", greater_than, param_placeholder),
	"exponentiate" : SubOperation("exponentiate", exponentiate, param_placeholder)
	}
	
	"""
	Args:
	    (Dict) operation_columns_to_sub_operation: Keys are the subset of all columns that 
	    	will be operated on by an operator. Values are the operation to be applied for
	    	the column. Operations are of type SubOperation that take as input the value they need.
	Returns:
		None
	Raises:
	    None
	"""
	def __init__(self, column_name, sub_operation_name):		
		self.sub_operation_name = sub_operation_name
		self.column_name = column_name
		self.sub_operation = RowOperation.possible_operations[sub_operation_name]

	def is_valid_operation(self, operation):
		if operation not in operation_types.keys():
			raise Exception("Unkown operation: {}".format(operation)) 

	def get_possible_operations(self):
		return "The possible operations are : {}".format(operation_types.keys())

	"""
	Args:
		(Pandas DF) dataset: the dataset to be operated upon.
	Returns:
		(Pandas DF) output: the post-operation dataset.
	Raises:
	    None
	"""
	def execute(self, dataset):
		output_df = dataset.copy()
		output_df[self.column_name] = output_df[self.column_name].apply(self.sub_operation.execute)
		return output_df

	def __str__(self):
		return "Row operation (" + self.sub_operation_name + ")"
#SMALL TEST
# df = pd.DataFrame([[74, 200, 22, "Alex"],[71, 140, 19, "Shea"], [75, 170, 20, "Abby"]], columns = ['height', 'weight', 'age', 'name'])
# df.loc[3] = {"height":78, "name":"Future Alex", "weight":105, "age":25}
# row_op = RowOperation("height", "exponentiate")
# print row_op.execute(df)












