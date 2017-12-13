from Operation import Operation
import pandas as pd
from SubOperation import SubOperation
import TransformationOperationModule as tr
class TransformationOperation(Operation):

	"""
	Transformation operations take in a dataset and output a new dataset with fewer rows, but no less than 2.
	Transformation operations are similar to Aggregation operations, except that transformation operations must
	return a dataframe with more than 2 rows and an aggregation operaton must return a single row.
	"""

	"""
	Args:
		transformation_operation_name (String): The correpsonding operation to apply to the data based
			on the operations available in possible_operations dict defined above.
	Returns:
		None
	Raises:
	    None
	"""
	def __init__(self, column_name, transformation_operation_name):
		self.transformation_operation_name = transformation_operation_name
		self.transformation_operation = tr.possible_operations[transformation_operation_name]
		self.column_name = column_name

	def preprocess(self, table_meta):
		self.input_meta = table_meta.copy()
		dtype = self.input_meta.get_type(self.column_name)
		for itype, otype in tr.operation_io_types[self.transformation_operation_name]:
			if dtype == itype:
				table_meta.set_type(self.column_name, otype)
				return table_meta
		return None

	def execute(self, dataset):
		if self.operation_requires_column_name():
			return self.transformation_operation(dataset, self.column_name)
		else:
			return self.transformation_operation(dataset)

	def operation_requires_column_name(self):
		if self.transformation_operation_name == "diff":
			return True
		else:
			return False

	def __str__(self):
		return "Transformation operation (" + self.column_name + " " + self.transformation_operation_name + ")"

#SMALL TEST
# df = pd.DataFrame([[74, 200, 22, "Alex"],[71, 140, 19, "Shea"], [75, 170, 20, "Abby"]], columns = ['height', 'weight', 'age', 'name'])
# df.loc[3] = {"height":78, "name":"Future Alex", "weight":105, "age":25}
# trans_op = TransformationOperation("height", "diff")
# df =  trans_op.execute(df)
# print df
