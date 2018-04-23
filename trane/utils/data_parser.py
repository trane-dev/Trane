from datetime import datetime
from functools import reduce

import pandas as pd

from .table_meta import TableMeta as TM

__all__ = ['df_group_by_entity_id', 'csv_to_df', 'parse_data']


def df_group_by_entity_id(dataframe, entity_id_column_name):
    """
    Convert a dataframe with an entity_id column to a dictionary mapping entity id's to their relevant data.

    Parameters
    ----------
    dataframe: the data
    entity_id_column: the column name with entity id's

    Returns
    ----------
    dict: Mapping from entity id's to a dataframe containing only that entity id's data.
    """

    df_groupby = dataframe.groupby(entity_id_column_name)
    entities = df_groupby.groups.keys()
    entity_id_to_df = [(key, df_groupby.get_group(key)) for key in entities]
    return dict(entity_id_to_df)


def csv_to_df(csv_filenames, header=True):
    """
    Convert csv's to a dataframe

    Parameters
    ----------
    csv_filenames: a list of csv filepaths
    header: does the data have a header/column names at the top of the file?

    Returns
    ----------
    dataframe: a merge of all the csv's
    """
    if not header:
        dataframes = [pd.read_csv(file_path, header=None)
                      for file_path in csv_filenames]
    else:
        dataframes = [pd.read_csv(file_path) for file_path in csv_filenames]

    merged_df = reduce((lambda left_frame, right_frame: pd.merge(
        left_frame, right_frame, how='outer')), dataframes)

    return merged_df


def parse_data(dataframe, table_meta):
    """
    Convert columns specified as time in the table_meta from str objects to datetime objects.

    Parameters
    ----------
    dataframe: the data
    table_meta: a TableMeta object specifying meta information about the data

    Returns
    ----------
    dataframe: with time columns converted from str to datetime.
    """

    columns = table_meta.get_columns()
    for column in columns:
        if table_meta.get_type(column) == TM.TYPE_TIME:
            dataframe[column] = dataframe[column].apply(
                lambda x: datetime.strptime(x, table_meta.get_property(column, "format")))
    return dataframe
