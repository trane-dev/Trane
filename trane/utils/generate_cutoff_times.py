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

            entity_training_cutoff, entity_label_cutoff = self.get_cutoff(
                entity_data, time_column)

            entity_to_data_and_cutoff_dict[entity] = (
                entity_data, entity_training_cutoff, entity_label_cutoff)

        return entity_to_data_and_cutoff_dict


class ConstantCutoffTime(CutoffTimeBase):

    def __init__(self, training_cutoff, label_cutoff):
        self.training_cutoff = training_cutoff
        self.label_cutoff = label_cutoff

    def get_cutoff(self, entity_data, time_column):
        return self.training_cutoff, self.label_cutoff


class DynamicCutoffTime(CutoffTimeBase):
    """
    DynamicCutoffTime use (1 - training_label_ratio - test_label_ratio) * N records
    to generate features.
    use the following training_label_ratio * N records for training labels.
    use the last test_label_ratio * N records for testing labels.
    """

    def __init__(self, training_label_ratio=.2, test_label_ratio=.2):
        assert training_label_ratio + test_label_ratio < 1
        assert training_label_ratio > 0 and test_label_ratio > 0
        self.training_label_ratio = training_label_ratio
        self.test_label_ratio = test_label_ratio

    def get_cutoff(self, entity_data, time_column):
        timestamps = entity_data[time_column].copy()
        timestamps = timestamps.sort_values()

        N = len(timestamps)

        training_cutoff_index = int((1 - self.training_label_ratio - self.test_label_ratio) * N)
        label_cutoff_index = int((1 - self.test_label_ratio) * N)

        return timestamps.iloc[training_cutoff_index], timestamps.iloc[label_cutoff_index]




