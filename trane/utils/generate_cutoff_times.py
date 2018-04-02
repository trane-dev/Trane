# IMPORTANT INFO ABOUT FILE STRUCTURE:
# Each cutoff strategy will be it's own class.
# The only function that should change across classes is
# apply_cutoff_strategy()

__all__ = ["CutoffTimeBase", "ConstantCutoffTime"]


class CutoffTimeBase:

    def __init__(self):
        pass
    """Args:
		entity_to_data_dict(Dict): mapping from entities to their data
	
	Returns:
		entity_to_data_and_cutoff_dict(Dict): mapping from entities to a tuple of data and cutoff time

	"""

    def generate_cutoffs(self, entity_to_data_dict):
        entity_to_data_and_cutoff_dict = {}
        for entity in entity_to_data_dict:
            entity_data = entity_to_data_dict[entity]
    
            entity_training_cutoff = self.get_training_cutoff(entity_data)
            entity_label_cutoff = self.get_label_cutoff(entity_data)
    
            entity_to_data_and_cutoff_dict[entity] = (entity_data, entity_training_cutoff, entity_label_cutoff)
    
        return entity_to_data_and_cutoff_dict

class ConstantCutoffTime(CutoffTimeBase):

    def __init__(self, training_cutoff, label_cutoff):
        self.training_cutoff = training_cutoff
        self.label_cutoff = label_cutoff

    def get_training_cutoff(self, entity_data):
        return self.training_cutoff
    def get_label_cutoff(self, entity_data):
        return self.label_cutoff


