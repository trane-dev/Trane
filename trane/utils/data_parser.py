from typing import Dict, List, Tuple

import pandas as pd


def denormalize(
    dataframes: Dict[str, pd.DataFrame],
    relationships: List[Tuple[str, str, str, str]],
) -> pd.DataFrame:
    """
    Convert a list of dataframes into a single dataframe
    according to relationships (between the dataframes).

    Arguments:
        dataframes: a list of tuples, each tuple contains a table name and a dataframe.
        relationships: a list of tuples, each tuple contains a table name and the corresponding index name.
            Can be used for one to many, one to one or many to many relationship is defined.
            [(parent_table_name, parent_join_key, child_table_name, child_join_key)].
    Returns:
        dataframe: a merge of all the dataframes according to the relationships
    """
    merged_dataframes = []
    for relationship in relationships:
        parent_table_name, parent_key, child_table_name, child_key = relationship

        parent_table = dataframes[parent_table_name]
        child_table = dataframes[child_table_name]
        parent_from_list = False
        child_from_list = False
        if parent_table_name in [x[0] for x in merged_dataframes]:
            # have already used it as a parent before, so use the merged version
            parent_table = [
                x[1] for x in merged_dataframes if x[0] == parent_table_name
            ][0]
            merged_dataframes.remove((parent_table_name, parent_table))
            parent_from_list = True

        if child_table_name in [x[0] for x in merged_dataframes]:
            # have already used it as a child before, so use the merged version
            child_table = [x[1] for x in merged_dataframes if x[0] == child_table_name][
                0
            ]
            merged_dataframes.remove((child_table_name, child_table))
            child_from_list = True

        flat = (
            parent_table.set_index(parent_key)
            .merge(
                child_table.set_index(child_key),
                how="right",
                left_index=True,
                right_index=True,
            )
            .reset_index()
        )
        if parent_from_list:
            merged_dataframes.remove((parent_table_name, parent_table))
        if child_from_list:
            pass
            # merged_dataframes.remove((child_table_name, child_table))
        merged_dataframes.append((parent_table_name, flat))
    if len(merged_dataframes) == 1:
        return merged_dataframes[0][1]
    raise NotImplementedError(
        "Not implemented for more than one to many relationships, with two dataframes",
    )


# class CsvMerge:
# """
# Simple class for helping with csv merge operations.
# """

# def __init__(self, csv_names, data):
#     self.csv_names = csv_names
#     self.data = data
#     def contains_relevant_csv_name(self, name_to_check):
#         return name_to_check in self.csv_names

#     def get_data(self):
#         return self.data
#     def get_csv_names(self):
#         return self.csv_names
#     def merge_objects(self, other, left_key, right_key):
#         csv_names = self.get_csv_names() + other.get_csv_names()
#         data = pd.merge(
#             self.get_data(),
#             other.get_data(),
#             left_on=left_key,
#             right_on=right_key,
#         )
#         return CsvMerge(csv_names, data)
# def denormalize(dataframes : Dict[str, pd.DataFrame],
#                 relationships: List[Tuple [str, str, str, str]]) -> pd.DataFrame:
#     """
#     Convert csv's to a dataframe according to relationships. Note, the function
#         assumes that the csv's have header columns.
#     Parameters
#     ----------
#     relationships: a list containing all the relationships among the csv's.
#         the relationships are defined in a list as follows:
#         here is how a one to many, one to one or many to many relationship is defined.
#         [(parent_csv, parent_join_key, child_csv, child_join_key)].
#     Returns
#     ----------
#     dataframe: a merge of all the csv's according to the relationships defined in relationships.
#     """
#     csv_merge_objs = []
#     for relationship in relationships:
#         parent_table_name, parent_key, child_table_name, child_key = relationship

#         parent_table = dataframes[parent_table_name]
#         child_table = dataframes[child_table_name]

#         parent_from_list = False
#         child_from_list = False
#         for csv_merge_obj in csv_merge_objs:
#             if csv_merge_obj.contains_relevant_csv_name(parent_key):
#                 parent_table = csv_merge_obj
#                 parent_from_list = True
#             if csv_merge_obj.contains_relevant_csv_name(child_key):
#                 child_table = csv_merge_obj
#                 child_from_list = True
#         new_merge_obj = pd.merge(
#             parent_table,
#             child_table,
#             left_on=parent_key,
#             right_on=child_key,
#         )
#         if parent_from_list:
#             csv_merge_objs.remove(parent_table)
#         if child_from_list:
#             csv_merge_objs.remove(child_table)
#         csv_merge_objs.append(new_merge_obj)
#     assert len(csv_merge_objs) == 1
#     return csv_merge_objs[0].get_data()
