from typing import Dict, List, Tuple

import pandas as pd


def denormalize(
    dataframes: Dict[str, Tuple[pd.DataFrame, str]],
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
    check_target_entity(target_entity, relationships, list(dataframes.keys()))
    relationship_order = child_relationships(target_entity, relationships)
    relationship_order = reorder_relationships(target_entity, relationship_order)

    for relationship in relationship_order:
        parent_table_name, parent_key, child_table_name, child_key = relationship

        parent_table = dataframes[parent_table_name]
        child_table = dataframes[child_table_name]

        if parent_table_name in merged_dataframes:
            # have already used it as a parent before, so use the merged version (it has more information)
            parent_table = merged_dataframes[parent_table_name]
            merged_dataframes.pop(parent_table_name)
        if child_table_name in merged_dataframes:
            # have already used it as a child before, so use the merged version (it has more infomation)
            child_table = merged_dataframes[child_table_name]
            merged_dataframes.pop(child_table_name)

        parent_table = parent_table.add_prefix(parent_table_name + ".")
        parent_key = parent_table_name + "." + parent_key

        flat = flatten_dataframes(parent_table, child_table, parent_key, child_key)
        merged_dataframes[child_table_name] = flat
        merged_dataframes[parent_table_name] = flat
    # TODO: set primary key to be the index
    # TODO: pass information to table meta (primary key, foreign keys)? maybe? technically relationships has this info
    return merged_dataframes[target_entity]


def flatten_dataframes(parent_table, child_table, parent_key, child_key):
    parent_table = parent_table.set_index(parent_key, inplace=False)
    child_table = child_table.set_index(child_key, inplace=False)
    return parent_table.merge(
        child_table,
        # right = we want to keep all the rows in the child table
        how="right",
        left_index=True,
        right_index=True,
        validate="one_to_many",
    ).reset_index(names=child_key)


def child_relationships(parent_table, relationships):
    # dfs implemented iteratively
    visited = set()
    children_relationships = []
    stack = [parent_table]
    while len(stack) > 0:
        table = stack.pop()
        if table not in visited:
            visited.add(table)
        for relationship in relationships:
            parent_table_name, _, child_table_name, _ = relationship
            if child_table_name == table:
                children_relationships.append(relationship)
                stack.append(parent_table_name)
    return children_relationships


def reorder_relationships(target_entity, relationships):
    reordered_relationships = []
    for relationship in relationships:
        parent_table_name, parent_key, child_table_name, child_key = relationship
        if child_table_name == target_entity:
            reordered_relationships.append(relationship)
        else:
            reordered_relationships.insert(0, relationship)
    return reordered_relationships


def check_target_entity(target_entity, relationships, dataframe_names):
    if target_entity not in [x[2] for x in relationships]:
        raise ValueError(f"{target_entity} not in relationships: {relationships}")
    if target_entity not in dataframe_names:
        raise ValueError(f"{target_entity} not in dataframes: {dataframe_names}")
