import logging
import time

import pandas as pd

from .prediction_problem_saver import prediction_problems_from_json_file

__all__ = ['Labeler']


class Labeler():
    """
    Object for executing prediction problems on data in order
    to generate labels for many prediction problems.
    The execute method performs the labelling operation.
    """

    def __init__(self):
        pass

    def execute(self, entity_to_data_and_cutoff_dict,
                json_prediction_problems_filename):
        """
        Generate the labels.

        Parameters
        ----------
        entity_to_data_and_cutoff_dict: mapping from
            unique entities to their related data and cutoff times
        json_prediction_problems_filename: filename to read
            prediction problems from, structured in JSON.

        Returns
        ----------
        dfs: a list of DataFrames. One dataframe for each problem.
            Each dataframe contains entities, cutoff times and labels.

        """

        prediction_problems, table_meta, entity_id_column, label_generating_column, time_column = \
            prediction_problems_from_json_file(
                json_prediction_problems_filename)

        dfs = []
        columns = [entity_id_column, 'training_labels',
                   'test_labels', 'training_cutoff_time', 'label_cutoff_time']

        for idx, prediction_problem in enumerate(prediction_problems):
            start = time.time()
            df_rows = []
            logging.debug("in labeller and beginning exuection of problem: {} \n".format(
                prediction_problem))

            for entity in entity_to_data_and_cutoff_dict:

                df_row = []
                entity_data, training_cutoff, label_cutoff = entity_to_data_and_cutoff_dict[
                    entity]

                df_pre_label_cutoff_time_result, df_all_data_result = prediction_problem.execute(
                    entity_data, time_column, label_cutoff,
                    prediction_problem.filter_column_order_of_types,
                    prediction_problem.label_generating_column_order_of_types)

                if len(df_pre_label_cutoff_time_result) == 1:
                    label_precutoff_time = df_pre_label_cutoff_time_result[
                        label_generating_column].values[0]
                elif len(df_pre_label_cutoff_time_result) > 1:
                    logging.warning("Received output from prediction problem \
                                    execution on pre-label cutoff data with more than one result.")
                    label_precutoff_time = None
                else:
                    label_precutoff_time = None
                if len(df_all_data_result) == 1:
                    label_postcutoff_time = df_all_data_result[
                        label_generating_column].values[0]
                elif len(df_all_data_result) > 1:
                    logging.warning("Received output from prediction problem execution \
                                     on all data with more than one result.")
                    label_postcutoff_time = None
                else:
                    label_postcutoff_time = None

                df_row = [entity, label_precutoff_time,
                          label_postcutoff_time, training_cutoff, label_cutoff]
                df_rows.append(df_row)
            df = pd.DataFrame(df_rows, columns=columns)
            end = time.time()
            logging.info("Finished labelling problem: {} of {}. Time elapsed: {}".format(idx, len(prediction_problems), end - start))
            dfs.append(df)

        return dfs
