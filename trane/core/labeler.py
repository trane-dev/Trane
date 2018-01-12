import json
from .prediction_problem import PredictionProblem
from .prediction_problem_saver import *

__all__ = ['Labeler']

class Labeler():

    def __init__(self):
        pass
    
    """
    Loads the data located at the file address: json_data.
        Args:
            entity_to_data_and_cutoff_dict: A dictionary mapping the entity id to a tuple of (data, cutoff_time)
            json_prediction_problems_filename: filename where the prediction problems are stored in a JSON format
        Returns:
            (Dict): A mapping from entity id to an tuple of ([labels (one for every prediction problem)], cutoff_time)
        """
    def execute(self, entity_to_data_and_cutoff_dict, json_prediction_problems_filename):
        """Generate the labels for each entity.
        Args:
        """
        with open(json_prediction_problems_filename) as f:
            prediction_problems, table_meta, entity_id_column, label_generating_column, time_column = \
                prediction_problems_from_json(f.read())
        
        entity_id_to_labels_and_cutoffs = {}
        for entity in entity_to_data_and_cutoff_dict:
            entity_data, entity_cutoff = entity_to_data_and_cutoff_dict[entity]
            entity_labels = []
            for prediction_problem in prediction_problems:
                execution_result = prediction_problem.execute(entity_data, time_column, entity_cutoff)
                assert len(execution_result) == 1
                execution_result = execution_result[label_generating_column].values[0]
                entity_labels.append(execution_result)

            entity_id_to_labels_and_cutoffs[entity] = (entity_labels, entity_cutoff)

        return entity_id_to_labels_and_cutoffs
