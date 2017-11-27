from Operation import Operation
import pandas as pd
from SubOperation import SubOperation
import FilterOperationModule as fo
class FilterOperation(Operation):

	"""
	A Filter takes in as input a dataset. The filter then applies functions and compares them to thresholds
	If true that row remains, otherwise the row is removed. The output is the filtered dataset.
	"""

	"""
	Args:
		column_name (String): The column within each row to check with the filter.
		sub_operation_name (String): The name of the SubOperation to perform. Chosen from the
			values in the possible_operations dictionary.
	Returns:
		None
	"""
	def __init__(self, column_name, sub_operation_name):
		self.sub_operation_name = sub_operation_name
		self.filter_suboperation = fo.possible_operations[sub_operation_name]
		self.column_name = column_name

	def execute(self, dataset):
		output_df = dataset.copy()
		drop_indices = []
		for idx, row in dataset.iterrows():
			value = row[self.column_name]

			keep_row = self.filter_suboperation.execute(value)
			if not keep_row:
				drop_indices.append(idx)

		output_df = output_df.drop(drop_indices)
		return output_df

	def __str__(self):
		return "Filter operation (" + self.sub_operation_name + ")"

#TEST ----
# gt_filter = FilterOperation("height", "greater than")
# df = pd.DataFrame([[74, 200, 22, "Alex"],[71, 140, 19, "Shea"], [75, 170, 20, "Abby"]], columns = ['height', 'weight', 'age', 'name'])
# df.loc[3] = {"height":78, "name":"Future Alex", "weight":105, "age":25}
# print(gt_filter.execute(df))
# print(gt_filter)
