import pandas as pd

from trane.metadata import BaseMetadata, SingleTableMetadata
from trane.parsing.denormalize import (
    child_relationships,
    reorder_relationships,
)


class ProblemGenerator:
    metadata = None
    target_table = None
    window_size = None
    entity_columns = []
    problem_type = ["classification", "regression"]

    def __init__(
        self,
        metadata,
        window_size,
        target_table,
        entity_columns,
        problem_type,
    ):
        if not issubclass(metadata, BaseMetadata):
            raise ValueError("metadata is not a valid type")
        metadata.check_if_table_exists(target_table)

        self.metadata = metadata
        self.window_size = window_size
        self.target_table = target_table
        self.entity_columns = entity_columns
        self.problem_type = problem_type

    def generate(self):
        # denormalize and create single metadata table
        denormalize_metadata(
            self.metadata,
            self.target_table,
        )


def denormalize_metadata(metadata, target_table: str):
    column_to_ml_type = {}
    merged_dataframes = {}
    new_to_original_col = {}

    relationship_order = child_relationships(target_table, metadata.relationships)
    relationship_order = reorder_relationships(target_table, relationship_order)

    for rel in relationship_order:
        parent_table_name, parent_key, child_table_name, child_key = rel

        if parent_table_name in merged_dataframes:
            # have already used it as a parent before, so use the merged version (it has more information)
            parent_table = merged_dataframes[parent_table_name]
            merged_dataframes.pop(parent_table_name)
        else:
            parent_table = pd.DataFrame(
                columns=metadata.ml_types[parent_table_name].keys(),
            )
        original_columns = list(parent_table.columns)

        if child_table_name in merged_dataframes:
            # have already used it as a child before, so use the merged version (it has more infomation)
            child_table = merged_dataframes[child_table_name]
            merged_dataframes.pop(child_table_name)
        else:
            child_table = pd.DataFrame(
                columns=metadata.ml_types[child_table_name].keys(),
            )
            column_to_ml_type.update(metadata.ml_types[child_table_name])

        parent_table = parent_table.add_prefix(parent_table_name + ".")
        parent_key = parent_table_name + "." + parent_key
        new_to_original_col.update(
            {parent_table_name + "." + col: col for col in original_columns},
        )
        for new_col in parent_table.columns:
            original_col = new_to_original_col[new_col]
            if original_col in metadata.ml_types[parent_table_name]:
                ml_type = metadata.ml_types[parent_table_name][original_col]
            else:
                ml_type = column_to_ml_type[original_col]
                column_to_ml_type.pop(original_col)
            column_to_ml_type[new_col] = ml_type

        flat = flatten_dataframes(parent_table, child_table, parent_key, child_key)
        merged_dataframes[child_table_name] = flat
        merged_dataframes[parent_table_name] = flat

    valid_columns = list(merged_dataframes[target_table].columns)
    valid_ml_types = {col: column_to_ml_type[col] for col in valid_columns}
    return SingleTableMetadata(
        ml_types=valid_ml_types,
        index=metadata.indices.get(target_table, None),
        time_index=metadata.time_indices.get(target_table, None),
    )


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
