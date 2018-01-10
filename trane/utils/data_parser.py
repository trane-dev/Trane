import pandas as pd
import numpy as np
from functools import reduce

def df_to_entity_id_to_df(dataframe, entity_id_column_name):
    """
    Convert a dataframe with an entity_id column to a dictionary mapping entity id's to their relevant data.
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

def csv_to_df(csv_file_paths, header = True):
    """
    Args:
        (List)csv_file_paths: A list of the paths to the csv files you want to load
        (Boolean)header: are there headers to the data columns?
    Returns:
        (List[dataframe])dataframes: A list of dataframes converted from the csv's, in the same order they were input
    """
    if not header:
        dataframes = [pd.read_csv(file_path, header = None) for file_path in csv_file_paths]
    else:
       dataframes = [pd.read_csv(file_path) for file_path in csv_file_paths]

    return dataframes

def denormalize_dataframes(dataframes):
    """
    Args:
        (List)dataframes: A list of the dataframes to combine and denormalize
    Returns:
        Dataframe: a single de-normalized dataframe structured from all the input dataframes.
    """
    merged_df = reduce((lambda left_frame, right_frame: pd.merge(left_frame, right_frame, how = 'outer')), dataframes)
    merged_df.to_csv('merged_df.csv')
    return merged_df


if __name__ == "__main__":
    df = pd.read_csv('../../test_datasets/synthetic_taxi_data.csv')
    print(df)
    print(df_to_entity_id_to_df(df, 'taxi_id')[0])
    print(df_to_entity_id_to_df(df, 'taxi_id')[1])
    print(df_to_entity_id_to_df(df, 'taxi_id')[2])
