from Operation import Operation
import pandas as pd
from SubOperation import SubOperation
class TransformationOperation(Operation):
	
	"""
	Transformation operations take in a dataset and output a new dataset with fewer rows, but no less than 2.
	Transformation operations are similar to Aggregation operations, except that transformation operations must
	return a dataframe with more than 2 rows and an aggregation operaton must return a single row.
	"""


	"""
	Classwide methods:
	These are the Transformation operations possible under the Transformation Operation class.
	Methods can be added here under 2 constraints.
	1. Create a function with the dataframe as input and return a new dataframe.
	2. Add the function to the dictionary of possible operations.
	"""
	def identity(dataframe):
		return dataframe
	
	def diff(dataframe):
		output_df = pd.DataFrame(columns = list(dataframe))
		column_names = list(dataframe)
		for idx, row in dataframe.iterrows():
			if idx == 0:
				continue
			else:
				new_row = {}
				current_row = row
				previous_row = dataframe.iloc[idx-1]
				for column in column_names:
					if type(current_row[column]) is not str:
						diff = current_row[column] - previous_row[column]
					else:
						diff = current_row[column] + ' - ' + previous_row[column] #String Concat
					new_row[column] = diff
				output_df = output_df.append(new_row, ignore_index = True)
		return output_df
	
	possible_operations = {
		"identity" : identity,
		"diff": diff
	}


	"""
	Args:
		transformation_operation_name (String): The correpsonding operation to apply to the data based
			on the operations available in possible_operations dict defined above.
	Returns:
		None
	Raises:
	    None
	"""
	def __init__(self, transformation_operation_name):
		self.transformation_operation_name = transformation_operation_name
		self.transformation_operation = TransformationOperation.possible_operations[transformation_operation_name]
	
	def execute(self, dataset):
		return self.transformation_operation(dataset)

	def __str__(self):
		return "Transformation operation (" + self.transformation_operation_name + ")"

#SMALL TEST
# df = pd.DataFrame([[74, 200, 22, "Alex"],[71, 140, 19, "Shea"], [75, 170, 20, "Abby"]], columns = ['height', 'weight', 'age', 'name'])
# df.loc[3] = {"height":78, "name":"Future Alex", "weight":105, "age":25}
# trans_op = TransformationOperation("DIFF")
# print trans_op.execute(df)

