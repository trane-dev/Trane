from Operation import Operation
import pandas as pd
from SubOperation import SubOperation
class FilterOperation(Operation):
	
	#FILTER OPERATION CLASS
	#A Filter takes in as input a dataset. The filter then applies functions and compares them to thresholds
	#	If true that row remains, otherwise the row is removed. The output is the filtered dataset.

	def gt(value, param):
		return value > param
	def lt(value, param):
		return value < param
	
	"""
	Args:
		
	Returns:
		None
	Raises:
	    None
	"""
	def __init__(self, sub_operation, params):
		self.filter_suboperation = sub_operation
		self.filter_suboperation.set_params(params)


	def execute(self, column_to_filter_over, dataset):
		output_df = dataset.copy()
		drop_indices = []
		for idx, row in dataset.iterrows():
			value = row[column_to_filter_over]
			keep_row = self.filter_suboperation.execute(value)
			if not keep_row:
				drop_indices.append(idx)
		
		output_df = output_df.drop(drop_indices)
		return output_df
	
	def __str__(self):
		return "Filter Operation"

#TEST ----
# def gt(value, param):
# 	return value > param
# def lt(value, param):
# 	return value < param

# greater_than = SubOperation("Greater Than", gt)
# less_than = SubOperation("Less Than", lt)
# gt_filter = Filter(greater_than, 73)



# df = pd.DataFrame([[74, 200, 22, "Alex"],[71, 140, 19, "Shea"], [75, 170, 20, "Abby"]], columns = ['height', 'weight', 'age', 'name'])
# df.loc[3] = {"height":78, "name":"Future Alex", "weight":105, "age":25}

# print gt_filter.execute("height", df)