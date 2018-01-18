#IMPORTANT INFO ABOUT FILE STRUCTURE:
#Each cutoff strategy will be it's own class. 
#The only function that should change across classes is apply_cutoff_strategy()

class CutoffTimeBase:
	def __init__(self):
		pass
	"""
	Args:
		entity_to_data_dict(Dict): mapping from entities to their data
	Returns:
		entity_to_data_and_cutoff_dict(Dict): mapping from entities to a tuple of data and cutoff time
	"""
	def generate_cutoffs(self, entity_to_data_dict):
		entity_to_data_and_cutoff_dict = {}
		for entity in entity_to_data_dict:
			entity_data = entity_to_data_dict[entity]
			entity_cutoff = self.apply_cutoff_strategy(entity_data)
			#New dictionary insert
			entity_to_data_and_cutoff_dict[entity] = (entity_data, entity_cutoff)
		return entity_to_data_and_cutoff_dict

class FixedCutoffTimes(CutoffTimeBase):
	def apply_cutoff_strategy(self, entity_data):
		#Here is where a user defines their cutoff strategy. In this class example
		#we return a constant cutoff time. 
		return 0

class VariableCutoffTime(CutoffTimeBase):
	def apply_cutoff_strategy(self, entity_data):
		#Here is where a user defines their cutoff strategy. This class is an example
		# of using the entity_data to influence the cutoff time.
		return 0 + entity_data 