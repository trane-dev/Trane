import pandas as pd
import numpy as np
from functools import reduce
from datetime import datetime
from .table_meta import TableMeta as TM

__all__ = ['df_group_by_entity_id', 'csv_to_df', 'parse_data']


def df_group_by_entity_id(dataframe, entity_id_column_name):
    """Convert a dataframe with an entity_id column to a dictionary mapping entity id's to their relevant data.

    Args:
        (Dataframe): Dataframe containing all the data
        (String) entity_id_column_name: the column name with entity id's

    Returns:
        (Dict): Mapping from entity id's to a dataframe containing only that entity id's data.

    """
    df_groupby = dataframe.groupby(entity_id_column_name)
    entities = df_groupby.groups.keys()
    entity_id_to_df = [(key, df_groupby.get_group(key)) for key in entities]
    return dict(entity_id_to_df)


def csv_to_df(csv_filenames, output_filename=None, header=True):
    """Args:
        (List)csv_filenames: A list of the paths to the csv files you want to load
        (Boolean)header: are there headers to the data columns?

    Returns:
        Dataframe: a single de-normalized dataframe structured from all the input csv's.

    """
    if not header:
        dataframes = [pd.read_csv(file_path, header=None)
                      for file_path in csv_filenames]
    else:
        dataframes = [pd.read_csv(file_path) for file_path in csv_filenames]

    merged_df = reduce((lambda left_frame, right_frame: pd.merge(
        left_frame, right_frame, how='outer')), dataframes)
    if output_filename != None:
        merged_df.to_csv(output_filename)
    return merged_df


def parse_data(dataframe, table_meta):
    """convert column from str to correct object

    Args:
        (DataFrame)data_frame
        (TableMeta)table_meta

    Returns:
        Dataframe
    """

    columns = table_meta.get_columns()
    for column in columns:
        if table_meta.get_type(column) == TM.TYPE_TIME:
            dataframe[column] = dataframe[column].apply(
                lambda x: datetime.strptime(x, table_meta.get_property(column, "format")))
    return dataframe
