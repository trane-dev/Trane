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
        prediction_problems, table_meta, entity_id_column, label_generating_column, time_column = \
            prediction_problems_from_json_file(
                json_prediction_problems_filename)

        dfs = []
        columns = [entity_id_column, 'problem_label_excluding_data_post_label_cutoff_time',
                   'problem_label_all_data', 'training_cutoff_time', 'label_cutoff_time']

        for prediction_problem in prediction_problems:

            df_rows = []
            logging.debug("in labeller and beginning exuection of problem: {} \n".format(
                prediction_problem))

            for entity in entity_to_data_and_cutoff_dict:

                df_row = []
                entity_data, training_cutoff, label_cutoff = entity_to_data_and_cutoff_dict[entity]

                df_pre_label_cutoff_time_result, df_all_data_result = prediction_problem.execute(
                                                                    entity_data, time_column, label_cutoff,
                                                                    prediction_problem.filter_column_order_of_types,
                                                                    prediction_problem.label_generating_column_order_of_types)

                if len(df_pre_label_cutoff_time_result) == 1:
                    label_precutoff_time = df_pre_label_cutoff_time_result[label_generating_column].values[0]
                else:
                    logging.warning("Received output from prediction problem \
                                    execution on pre-label cutoff data with more than one result.")
                    label_precutoff_time = None
                if len(df_all_data_result) == 1:
                    label_postcutoff_time = df_all_data_result[label_generating_column].values[0]
                else:
                    logging.warning("Received output from prediction problem execution \
                                     on all data with more than one result.")
                    label_postcutoff_time = None

                df_row = [entity, label_precutoff_time,
                          label_postcutoff_time, training_cutoff, label_cutoff]
                df_rows.append(df_row)
            df = pd.DataFrame(df_rows, columns=columns)
            dfs.append(df)

        return dfs
