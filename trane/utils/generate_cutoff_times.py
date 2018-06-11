# IMPORTANT INFO ABOUT FILE STRUCTURE:
# Each cutoff strategy will be it's own class.
# The only function that should change across classes is
# apply_cutoff_strategy()


__all__ = ["CutoffTimeBase", "ConstantCutoffTime", "DynamicCutoffTime"]


class CutoffTimeBase:
    """
    Base class that all cutoff time strategies inherit.

    Make Your Own
    -------------
    Simply make a new class that follows the requirements below and issue a pull request.

    Requirements
    ------------
    get_cutoff: function that returns a training cutoff time and a label cutoff time.
    """

    def __init__(self):
        pass

    def generate_cutoffs(self, entity_to_data_dict, time_column):
        """

        Parameters
        ----------
        entity_to_data_dict: mapping from entities to their data

        Returns
        -------
        entity_to_data_and_cutoff_dict: mapping from entities to a tuple of their
            data and cutoff time
        """
        entity_to_data_and_cutoff_dict = {}
        for entity in entity_to_data_dict:
            entity_data = entity_to_data_dict[entity]

            entity_training_cutoff, entity_label_cutoff = self.get_cutoff(
                entity_data, time_column)

            entity_to_data_and_cutoff_dict[entity] = (
                entity_data, entity_training_cutoff, entity_label_cutoff)

        return entity_to_data_and_cutoff_dict


class ConstantCutoffTime(CutoffTimeBase):
    """
    Apply a constant cutoff time across all entities
    """

    def __init__(self, training_cutoff, label_cutoff):
        self.training_cutoff = training_cutoff
        self.label_cutoff = label_cutoff

    def get_cutoff(self, entity_data, time_column):
        return self.training_cutoff, self.label_cutoff


class DynamicCutoffTime(CutoffTimeBase):
    """
    DynamicCutoffTime uses 1 - training_label_ratio - test_label_ratio
    fraction of the data to generate training features.
    An additional training_label_ratio fraction of the data is
    used to label the training examples (1 - test_label_ratio)
    An additional test_label_ratio fraction of the data is used to label
    the test examples. (1 or all the data)
    """

    def __init__(self, training_label_ratio=.2, test_label_ratio=.2):
        self.training_label_ratio = training_label_ratio
        self.test_label_ratio = test_label_ratio

    def get_cutoff(self, entity_data, time_column):
        timestamps = entity_data[time_column].copy()
        timestamps = timestamps.sort_values()

        N = len(timestamps)

        training_cutoff_index = int((1 - self.training_label_ratio - self.test_label_ratio) * N)
        label_cutoff_index = int((1 - self.test_label_ratio) * N)

        return timestamps.iloc[training_cutoff_index], timestamps.iloc[label_cutoff_index]
