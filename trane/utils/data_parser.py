import pandas as pd
import numpy as np
from functools import reduce

__all__ = ['df_group_by_entity_id', 'csv_to_df']

def df_group_by_entity_id(dataframe, entity_id_column_name):
    """Convert a dataframe with an entity_id column to a dictionary mapping entity id's to their relevant data.
    
    Args:
        (Dataframe): Dataframe containing all the data
        (String) entity_id_column_name: the column name with entity id's

    Returns:
        (Dict): Mapping from entity id's to a dataframe containing only that entity id's data.
    
    """
    entity_ids = set(dataframe[entity_id_column_name])
    entity_id_to_df = {}
    for entity_id in entity_ids:
        relevant_data = dataframe.loc[dataframe[entity_id_column_name] == entity_id]
        entity_id_to_df[entity_id] = relevant_data
    return entity_id_to_df

def csv_to_df(csv_filenames, output_filename = None, header = True):
    """Args:
        (List)csv_filenames: A list of the paths to the csv files you want to load
        (Boolean)header: are there headers to the data columns?
    
    Returns:
        Dataframe: a single de-normalized dataframe structured from all the input csv's.
    
    """
    if not header:
        dataframes = [pd.read_csv(file_path, header = None) for file_path in csv_filenames]
    else:
       dataframes = [pd.read_csv(file_path) for file_path in csv_filenames]

    merged_df = reduce((lambda left_frame, right_frame: pd.merge(left_frame, right_frame, how = 'outer')), dataframes)
    if output_filename != None:
        merged_df.to_csv(output_filename)
    return merged_df
