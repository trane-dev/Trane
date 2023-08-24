from typing import Dict, List, Tuple, Union

import pandas as pd

from trane.metadata import MultiTableMetadata, SingleTableMetadata


def denormalize(
    metadata: Union[SingleTableMetadata, MultiTableMetadata],
    target_table: str,
    dataframes: Dict[str, pd.DataFrame] = None,
):
    """
    Convert a list of dataframes into a single dataframe
    according to relationships (between the dataframes).

    Arguments:
        dataframes: a list of tuples, each tuple contains a table name and a dataframe.
        metadata: metadata of the dataset.
    Returns:
        dataframe: a merge of all the dataframes according to the relationships
    """
    if dataframes is None:
        dataframes = {}
        if metadata.get_metadata_type() == "multi":
            for table_name in metadata.ml_types:
                columns = metadata.ml_types[table_name].keys()
                dataframes[table_name] = pd.DataFrame(columns=columns)

    dataframes, col_to_ml_types = denormalize_dataframes(
        dataframes,
        metadata.relationships,
        metadata.ml_types,
        target_table,
    )
    single_metadata = SingleTableMetadata(
        ml_types=col_to_ml_types,
        primary_key=metadata.primary_keys.get(target_table, None),
        time_index=metadata.time_primary_keys.get(target_table, None),
    )
    return dataframes[target_table], single_metadata


def denormalize_dataframes(
    dataframes: Dict[str, pd.DataFrame],
    relationships: List[Tuple[str, str, str, str]],
    ml_types: Dict[str, Dict[str, str]],
    target_table: str,
) -> pd.DataFrame:
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
    check_target_table(target_table, relationships, list(dataframes.keys()))
    relationship_order = child_relationships(target_table, relationships)
    relationship_order = reorder_relationships(target_table, relationship_order)

    column_to_ml_type = {}
    new_to_original_col = {}

    for relationship in relationship_order:
        parent_table_name, parent_key, child_table_name, child_key = relationship

        parent_table = dataframes[parent_table_name]
        child_table = dataframes[child_table_name]

        if parent_table_name in merged_dataframes:
            # have already used it as a parent before, so use the merged version (it has more information)
            parent_table = merged_dataframes[parent_table_name]
            merged_dataframes.pop(parent_table_name)
        original_columns = list(parent_table.columns)

        if child_table_name in merged_dataframes:
            # have already used it as a child before, so use the merged version (it has more infomation)
            child_table = merged_dataframes[child_table_name]
            merged_dataframes.pop(child_table_name)
        else:
            column_to_ml_type.update(ml_types[child_table_name])

        parent_table = parent_table.add_prefix(parent_table_name + ".")
        parent_key = parent_table_name + "." + parent_key

        new_to_original_col.update(
            {parent_table_name + "." + col: col for col in original_columns},
        )
        for new_col in parent_table.columns:
            original_col = new_to_original_col[new_col]
            if original_col in ml_types[parent_table_name]:
                ml_type = ml_types[parent_table_name][original_col]
            else:
                ml_type = column_to_ml_type[original_col]
                column_to_ml_type.pop(original_col)
            column_to_ml_type[new_col] = ml_type

        flat = flatten_dataframes(parent_table, child_table, parent_key, child_key)
        merged_dataframes[child_table_name] = flat
        merged_dataframes[parent_table_name] = flat
    # TODO: set primary key to be the index
    # TODO: pass information to table meta (primary key, foreign keys)? maybe? technically relationships has this info
    valid_columns = list(merged_dataframes[target_table].columns)
    col_to_ml_type = {col: column_to_ml_type[col] for col in valid_columns}
    return merged_dataframes, col_to_ml_type


def flatten_dataframes(parent_table, child_table, parent_key, child_key):
    parent_table.set_index(parent_key, inplace=True)
    child_table.set_index(child_key, inplace=True)
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


def reorder_relationships(target_table, relationships):
    reordered_relationships = []
    for relationship in relationships:
        parent_table_name, parent_key, child_table_name, child_key = relationship
        if child_table_name == target_table:
            reordered_relationships.append(relationship)
        else:
            reordered_relationships.insert(0, relationship)
    return reordered_relationships


def check_target_table(target_table, relationships, dataframe_names):
    if target_table not in [x[2] for x in relationships]:
        raise ValueError(f"{target_table} not in relationships: {relationships}")
    if target_table not in dataframe_names:
        raise ValueError(f"{target_table} not in dataframes: {dataframe_names}")
