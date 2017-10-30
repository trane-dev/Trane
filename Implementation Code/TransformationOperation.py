from Operation import Operation
import pandas as pd
from SubOperation import SubOperation
class TransformationOperation(Operation):
	
	#MULTIROW OPERATION TO BE APPLIED TO ALL COLUMNS FOR ALL ENTITIES
	#TRANFORMS ALL ROWS INTO A SINGLE ROW

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
						print diff
					else:
						diff = current_row[column] + ' - ' + previous_row[column] #String Concat
					new_row[column] = diff
				output_df = output_df.append(new_row, ignore_index = True)
		return output_df
	
	possible_operations = {
		"IDENTITY" : identity,
		"DIFF": diff
	}


	"""
	Args:
		
	Returns:
		None
	Raises:
	    None
	"""
	def __init__(self, transformation_operation):
		self.transformation_operation = transformation_operation
	
	def execute(self, dataset):
		return self.transformation_operation(dataset)

	def __str__(self):
		return "Transformation Operation"
#SMALL TEST
# df = pd.DataFrame([[74, 200, 22, "Alex"],[71, 140, 19, "Shea"], [75, 170, 20, "Abby"]], columns = ['height', 'weight', 'age', 'name'])
# df.loc[3] = {"height":78, "name":"Future Alex", "weight":105, "age":25}
# trans_op = TransformationOperation(TransformationOperation.possible_ag_opps["DIFF"])
# print df
# print trans_op.execute(df)
# print df
