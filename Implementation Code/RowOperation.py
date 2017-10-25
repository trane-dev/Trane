from Operation import Operation
import pandas as pd
from SubOperation import SubOperation
class RowOperation(Operation):
	
	def identity(val):
		return val
	possible_operations = {
	"Identity" : SubOperation("Identity", identity)
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
	def __init__(self, operation_columns_to_sub_operation):		
		self.operation_columns_to_sub_operation = operation_columns_to_sub_operation

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
		output_df = pd.DataFrame(columns = self.operation_columns_to_sub_operation.keys())
		for idx, row in dataset.iterrows():
			new_row = {}
			for column_name, sub_operation_name in self.operation_columns_to_sub_operation.iteritems():
				row_value = row[column_name]
				sub_operation = self.possible_operations[sub_operation_name]
				new_row[column_name] = sub_operation.execute(row_value)


			output_df.loc[idx] = new_row
		return output_df

#SMALL TEST
# df = pd.DataFrame([[74, 200, 22, "Alex"],[71, 140, 19, "Shea"], [75, 170, 20, "Abby"]], columns = ['height', 'weight', 'age', 'name'])
# df.loc[3] = {"height":78, "name":"Future Alex", "weight":105, "age":25}
# row_op = RowOperation(["height", "name", "weight", "age"], {"age":"Identity", "name":"Identity"})
# print row_op.execute(df)












