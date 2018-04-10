# IMPORTANT INFO ABOUT FILE STRUCTURE:
# Each cutoff strategy will be it's own class.
# The only function that should change across classes is
# apply_cutoff_strategy()

import numpy as np

__all__ = ["CutoffTimeBase", "ConstantCutoffTime", "DynamicCutoffTime"]


class CutoffTimeBase:

    def __init__(self):
        pass
    """Args:
		entity_to_data_dict(Dict): mapping from entities to their data
	
	Returns:
		entity_to_data_and_cutoff_dict(Dict): mapping from entities to a tuple of data and cutoff time

	"""

    def generate_cutoffs(self, entity_to_data_dict, time_column):
        entity_to_data_and_cutoff_dict = {}
        for entity in entity_to_data_dict:
            entity_data = entity_to_data_dict[entity]
    
            entity_training_cutoff, entity_label_cutoff = self.get_cutoff(entity_data, time_column)
    
            entity_to_data_and_cutoff_dict[entity] = (entity_data, entity_training_cutoff, entity_label_cutoff)
    
        return entity_to_data_and_cutoff_dict

class ConstantCutoffTime(CutoffTimeBase):

    def __init__(self, training_cutoff, label_cutoff):
        self.training_cutoff = training_cutoff
        self.label_cutoff = label_cutoff

    def get_cutoff(self, entity_data, time_column):
        return self.training_cutoff, self.label_cutoff
        

class DynamicCutoffTime(CutoffTimeBase):
    """
    DynamicCutoffTime use (1 - training_ratio - label_ratio) * N records 
    to generate features.
    use the following training_ratio * N records for training labels.
    use the last label_ratio * N records for testing labels.
    """
    
    def __init__(self, training_ratio=.2, label_ratio=.2):
        assert training_ratio + label_ratio < 1
        assert training_ratio > 0 and label_ratio > 0
        self.training_ratio = training_ratio
        self.label_ratio = label_ratio
        
    def get_cutoff(self, entity_data, time_column):
        timestemps = entity_data[time_column].copy()
        timestemps = timestemps.sort_values()
        
        N = len(timestemps)
        n_label = int(np.ceil(N * self.label_ratio))
        n_training_label = int(np.ceil(N * (self.training_ratio + self.label_ratio)))
        n_feature = N - n_training_label
        n_training = n_training_label - n_label
        return timestemps.iloc[n_feature], timestemps.iloc[n_feature + n_training]
