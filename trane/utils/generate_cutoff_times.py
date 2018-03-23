# IMPORTANT INFO ABOUT FILE STRUCTURE:
# Each cutoff strategy will be it's own class.
# The only function that should change across classes is
# apply_cutoff_strategy()

__all__ = ["CutoffTimeBase", "ConstantDatetimeCutoffTime",
           "ConstantIntegerCutoffTimes"]


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
            entity_cutoff = self.apply_cutoff_strategy(entity_data)
            # New dictionary insert
            entity_to_data_and_cutoff_dict[
                entity
            ] = (entity_data, entity_cutoff)
        return entity_to_data_and_cutoff_dict


class ConstantIntegerCutoffTimes(CutoffTimeBase):

    def __init__(self, integer_cutoff):
        assert isinstance(integer_cutoff, int)
        self.integer_cutoff = integer_cutoff

    def apply_cutoff_strategy(self, entity_data):
        return self.integer_cutoff


class ConstantDatetimeCutoffTime(CutoffTimeBase):

    def __init__(self, datetime_cutoff):
        assert isinstance
        self.datetime_cutoff = datetime_cutoff

    def apply_cutoff_strategy(self, entity_data):
        return self.datetime_cutoff
