import json
from .prediction_problem import PredictionProblem
from .prediction_problem_saver import *
import pandas as pd
__all__ = ['Labeler']

class Labeler():

    def __init__(self):
        pass
    
    def execute(self, entity_to_data_and_cutoff_dict, json_prediction_problems_filename):
        """Loads the data located at the file address: json_data.
        
        Args:
            entity_to_data_and_cutoff_dict: A dictionary mapping the entity id to a tuple of (data, cutoff_time)
            json_prediction_problems_filename: filename where the prediction problems are stored in a JSON format
        
        Returns:
            (DataFrame): A mapping from entity id to an tuple of ([labels (one for every prediction problem)], cutoff_time)
        """
        # with open(json_prediction_problems_filename) as f:
        #     prediction_problems, table_meta, entity_id_column, label_generating_column, time_column = \
        #         prediction_problems_from_json(f.read())
        prediction_problems, table_meta, entity_id_column, label_generating_column, time_column = \
            prediction_problems_from_json_file(json_prediction_problems_filename)
        
        
        columns = ['Entity Id'] + ['Problem {}'.format(_ + 1) for _ in range(len(prediction_problems))] + ['Cutoff Time']
        df_rows = []

        for entity in entity_to_data_and_cutoff_dict:
            entity_data, entity_cutoff = entity_to_data_and_cutoff_dict[entity]
            df_row = [entity]

            for prediction_problem in prediction_problems:
                execution_result = prediction_problem.execute(entity_data, time_column, entity_cutoff)
                
                assert len(execution_result) <= 1
                if len(execution_result) == 1:
                    execution_result = execution_result[label_generating_column].values[0]
                else:
                    execution_result = None

                df_row.append(execution_result)
            df_row.append(entity_cutoff)
            df_rows.append(df_row)

        entity_labels_cutoffs_df = pd.DataFrame(df_rows, columns = columns)
        return entity_labels_cutoffs_df
