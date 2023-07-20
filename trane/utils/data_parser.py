from typing import Dict, List, Tuple

import pandas as pd


def denormalize(
    dataframes: Dict[str, pd.DataFrame],
    relationships: List[Tuple[str, str, str, str]],
    target_entity: str,
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
    merged_dataframes = {}
    for relationship in relationships:
        parent_table_name, parent_key, child_table_name, child_key = relationship
        if parent_key not in dataframes[parent_table_name].columns:
            raise ValueError(
                f"{parent_key} not in table: {parent_table_name}",
            )
        if child_key not in dataframes[child_table_name].columns:
            raise ValueError(
                f"{child_key} not in table: {child_table_name}",
            )
    relationship_order = []
    if target_entity is not None:
        check_target_entity(target_entity, relationships, dataframes)
        relationship_order = reorder_relationships(relationships, target_entity)
    else:
        relationship_order = relationships

    for relationship in relationship_order:
        parent_table_name, parent_key, child_table_name, child_key = relationship

        parent_table = dataframes[parent_table_name]
        child_table = dataframes[child_table_name]

        if parent_table_name in merged_dataframes:
            # have already used it as a parent before, so use the merged version (it has more information)
            parent_table, _, _, original_parent_key = merged_dataframes[
                parent_table_name
            ]
            merged_dataframes.pop(parent_table_name)
        if child_table_name in merged_dataframes:
            # have already used it as a child before, so use the merged version (it has more infomation)
            child_table, _, _, original_parent_key = merged_dataframes[child_table_name]
            merged_dataframes.pop(child_table_name)

        parent_table = parent_table.add_prefix(parent_table_name + ".")
        original_parent_key = parent_key
        parent_key = parent_table_name + "." + parent_key

        flat = flatten_dataframes(parent_table, child_table, parent_key, child_key)
        merged_dataframes[child_table_name] = (
            flat,
            parent_key,
            child_key,
            original_parent_key,
        )
        merged_dataframes[parent_table_name] = (
            flat,
            parent_key,
            child_key,
            original_parent_key,
        )
    return merged_dataframes[target_entity][0]


def flatten_dataframes(parent_table, child_table, parent_key, child_key):
    return (
        parent_table.set_index(parent_key)
        .merge(
            child_table.set_index(child_key),
            # right = we want to keep all the rows in the child table
            how="right",
            left_index=True,
            right_index=True,
            validate="one_to_many",
        )
        .reset_index(names=child_key)
    )


def reorder_relationships(relationships, target_entity):
    reordered_relationships = []
    for relationship in relationships:
        parent_table_name, parent_key, child_table_name, child_key = relationship
        if child_table_name == target_entity:
            reordered_relationships.append(relationship)
        else:
            reordered_relationships.insert(0, relationship)
    return reordered_relationships


def check_target_entity(target_entity, relationships, dataframes):
    if target_entity not in [x[2] for x in relationships]:
        raise ValueError(f"{target_entity} not in relationships: {relationships}")
    if target_entity not in dataframes:
        raise ValueError(f"{target_entity} not in dataframes: {dataframes}")


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
