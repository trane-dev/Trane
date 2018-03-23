import json
from .prediction_problem import PredictionProblem
from .prediction_problem_saver import *
import pandas as pd
__all__ = ['Labeler']
import logging


class Labeler():

    def __init__(self):
        pass

    def execute(self, entity_to_data_and_cutoff_dict, json_prediction_problems_filename):
        """Loads the data located at the file address: json_data.

        Args:
            entity_to_data_and_cutoff_dict: A dictionary mapping the entity id to a tuple of (data, cutoff_time)
            json_prediction_problems_filename: filename where the prediction problems are stored in a JSON format

        Returns:
            (List of Dataframes): list of dataframes. dataframes contain entity_id, label and cutoff time.
        """
        # with open(json_prediction_problems_filename) as f:
        #     prediction_problems, table_meta, entity_id_column, label_generating_column, time_column = \
        #         prediction_problems_from_json(f.read())
        prediction_problems, table_meta, entity_id_column, label_generating_column, time_column = \
            prediction_problems_from_json_file(
                json_prediction_problems_filename)

        dfs = []
        columns = [entity_id_column, 'problem_label_excluding_data_post_cutoff_time',
                   'problem_label_all_data', 'cutoff_time']
        for prediction_problem in prediction_problems:
            df_rows = []
            for entity in entity_to_data_and_cutoff_dict:
                df_row = []
                entity_data, entity_cutoff = entity_to_data_and_cutoff_dict[
                    entity]
                df_pre_cutoff_time_result, df_all_data_result = prediction_problem.execute(
                    entity_data, time_column, entity_cutoff)

                assert len(df_pre_cutoff_time_result) <= 1
                assert len(df_all_data_result) <= 1

                if len(df_pre_cutoff_time_result) == 1:
                    label_precutoff_time = df_pre_cutoff_time_result[
                        label_generating_column].values[0]
                else:
                    label_precutoff_time = None
                if len(df_all_data_result) == 1:
                    label_postcutoff_time = df_all_data_result[
                        label_generating_column].values[0]
                else:
                    label_postcutoff_time = None

                df_row = [entity, label_precutoff_time,
                          label_postcutoff_time, entity_cutoff]
                df_rows.append(df_row)
            df = pd.DataFrame(df_rows, columns=columns)
            dfs.append(df)

        return dfs
