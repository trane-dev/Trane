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
	def __init__(self, table_meta, operations, label_generating_column, entity_id_column, time_column, dataframe=None):
		self.table_meta = table_meta
		self.operations = operations
		self.label_generating_column = label_generating_column
		self.entity_id_column = entity_id_column
		self.time_column = time_column
		if dataframe is not None:
			self.entity_id_to_cutoff_time = self.determine_cutoff_time(dataframe)
		
		self.valid = True
		temp_meta = self.table_meta.copy()
		for op in operations:
			temp_meta = op.preprocess(temp_meta)
			if not temp_meta:
				self.valid = False
				break
		
		
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
	A method to get all unique entity id's.
	Args:
		None
	Returns:
		(Set) A set of unique entity id's
	"""
	def get_entity_ids(self):
		return set(dataframe[self.entity_id_column])
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
		unique_entity_ids = self.get_entity_ids()
		entity_id_to_cutoff_time = {}
		for entity_id in unique_entity_ids:
			df_entity_id = dataframe[dataframe[self.entity_id_column] == entity_id]
			first_time_observed = df_entity_id[self.time_column].min()
			last_time_observed = df_entity_id[self.time_column].max()
			total_time = last_time_observed - first_time_observed
			cutoff_time = first_time_observed + total_time/2.
			entity_id_to_cutoff_time[entity_id] = cutoff_time

		return entity_id_to_cutoff_time

	"""
	A method to set the cutoff time for all entity id's.
	Args:
		(??): A global cutoff_time
	Returns:
		None
	"""
	def set_global_cutoff_time(self, global_cutoff_time):
		unique_entity_ids = self.get_entity_ids()
		entity_id_to_cutoff_time = {}
		for entity_id in unique_entity_ids:
			entity_id_to_cutoff_time[entity_id] = global_cutoff_time
		self.entity_id_to_cutoff_time = entity_id_to_cutoff_time

	def get_entity_id_to_cutoff_time(self):
		return self.entity_id_to_cutoff_time
