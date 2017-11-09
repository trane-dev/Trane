import pandas as pd
class PredictionProblem:

	"""
	Prediction Problem is made up of a list of Operations. The list of operations delineate
	the order the operations will be applied in. 
	"""
	
	"""
	Args:
		(List) Operations: a list of operations (class Operation) that define the
			order in which operations should take place.
		(String) label_generating_column: the column of interest. This column
			will be solely used for performing operations against.
		(String) entity_id_column: the column with entity id's.
		(String) time_column: the name of the column containing time information.
	Returns:
		None
	"""
	def __init__(self, operations, label_generating_column, entity_id_column, time_column):
		self.operations = operations
		self.label_generating_column = label_generating_column
		self.entity_id_column = entity_id_column
		self.time_column = time_column

	"""
	This function executes all the operations on the dataframe and returns the output. The output
	should be structured as a single label/value per the Trane documentation.
	See paper: "What would a data scientist ask? Automatically formulating and solving predicton
	problems."
	Args:
		(Pandas DataFrame): the dataframe containing the data we wish to analyze.
	Returns:
		(Boolean/Float): The Label/Value of the prediction problem's formulation when applied to the data.
	"""
	def execute(self, dataframe):
		output = dataframe.copy()
		output = output[[self.label_generating_column]]
		for operation in self.operations:
			# print "output: \n" + str(output)
			output = operation.execute(output)
		return output
	"""
	Args:
		None
	Returns:
		A natural language text describing the prediction problem. 
	"""
	def __str__(self):
		description = ""
		last_op_idx = len(self.operations) - 1
		for idx, operation in enumerate(self.operations):
			description += str(operation)
			if idx != last_op_idx:
				description += "->"
		return description

	"""
	This function generates the cutoff times for each entity id and puts the dictionary in the
		prediction problem (self).
	Current implementation is simple. The cutoff time is halfway through the observed
		time period for each entity.
	Args:
		(Pandas DataFrame): the dataframe containing the data we wish to analyze.)
	Returns:
		(Dict): Entity Id to Cutoff time mapping. 
	"""
	def determine_cutoff_time(self, dataframe):
		unique_entity_ids = set(dataframe[self.entity_id_column])
		entity_id_to_cutoff_time = {}
		for entity_id in unique_entity_ids:
			df_entity_id = dataframe[dataframe[self.entity_id_column] == entity_id]
			first_time_observed = df_entity_id[self.time_column].min()
			last_time_observed = df_entity_id[self.time_column].max()
			total_time = last_time_observed - first_time_observed
			cutoff_time = first_time_observed + total_time/2.
			entity_id_to_cutoff_time[entity_id] = cutoff_time
		
		self.entity_id_to_cutoff_time = entity_id_to_cutoff_time

# df = pd.read_csv('../../test_datasets/synthetic_taxi_data.csv')
# prediction_problem = PredictionProblem([], 'fare', 'taxi_id', 'time')
# prediction_problem.determine_cutoff_time(df)











