from Operation import Operation
import pandas as pd
from SubOperation import SubOperation
class FilterOperation(Operation):
	
	#FILTER OPERATION CLASS
	#A Filter takes in as input a dataset. The filter then applies functions and compares them to thresholds
	#	If true that row remains, otherwise the row is removed. The output is the filtered dataset.


	def greater(value, param):
		return value > param
	#TODO DEFINE POSSIBLE PARAMETER VALUES HERE
	gt_params = [76]
	gt = SubOperation("greater than", greater)
	
	def lesser(value, param):
		return value < param
		
	lt_params = [100, 88]
	#TODO DEFINE POSSIBLE PARAMETER VALUES HERE
	lt = SubOperation("less than", lesser)

	possible_operations = {
	"greater than" : (gt, gt_params),
	"less than" : (lt, lt_params)
	}

	"""
	Args:
		
	Returns:
		None
	Raises:
	    None
	"""
	def __init__(self, column_name, sub_operation_name):
		self.filter_suboperation, params = FilterOperation.possible_operations[sub_operation_name]
		self.column_name = column_name
		self.filter_suboperation.set_params(params)

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
		return "Filter Operation"

#TEST ----
# gt_filter = FilterOperation("height", "greater than")
# df = pd.DataFrame([[74, 200, 22, "Alex"],[71, 140, 19, "Shea"], [75, 170, 20, "Abby"]], columns = ['height', 'weight', 'age', 'name'])
# df.loc[3] = {"height":78, "name":"Future Alex", "weight":105, "age":25}
# print gt_filter.execute(df)