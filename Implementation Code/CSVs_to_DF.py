import pandas as pd
import numpy as np
from functools import reduce
"""
Args:
    (List)csv_file_paths: A list of the paths to the csv files you want to load
    (Boolean)header: are there headers to the data columns?
Returns:
    (List[dataframe])dataframes: A list of dataframes converted from the csv's, in the same order they were input
Raises:
    None
"""
def csv_to_df(csv_file_paths, header = True):
    if not header:
        dataframes = [pd.read_csv(file_path, header = None) for file_path in csv_file_paths]    
    else:
	   dataframes = [pd.read_csv(file_path) for file_path in csv_file_paths]
	
    return dataframes
"""
Args:
    (List)dataframes: A list of the dataframes to combine and denormalize
Returns:
    Dataframe: a single de-normalized dataframe structured from all the input dataframes.
Raises:
    None
"""
def denormalize_dataframes(dataframes):
    merged_df = reduce((lambda left_frame, right_frame: pd.merge(left_frame, right_frame, how = 'outer')), dataframes)
    merged_df.to_csv('merged_df.csv')
    return merged_df



dfs = csv_to_df(['walmart_data/stores.csv', 'walmart_data/features.csv'])
denormalize_dataframes(dfs)
	
