import json
from .prediction_problem import PredictionProblem
from .prediction_problem_saver import from_json

__all__ = ['Labeller']

class Labeller():

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
            prediction_problems, entity_id_column, label_generating_column, time_column= from_json(f.read())
        
        entity_id_to_labels_and_cutoffs_list = []
        for problem in prediction_problems:
            entity_id_to_labels_and_cutoffs = {}
            for entity in entity_to_data_and_cutoff_dict:
                entity_data, entity_cutoff = entity_to_data_and_cutoff_dict[entity]
                entity_labels = []
                for prediction_problem in prediction_problems:
                    entity_label_for_this_prediction_problem = prediction_problem.execute(entity_data)
                    entity_labels.append(entity_label_for_this_prediction_problem)

                entity_id_to_labels_and_cutoffs[entity] = (entity_labels, entity_cutoff)
            entity_id_to_labels_and_cutoffs_list.append(entity_id_to_labels_and_cutoffs)

        return entity_id_to_labels_and_cutoffs_list
