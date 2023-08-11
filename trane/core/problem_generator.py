import pandas as pd

from trane.metadata import BaseMetadata, SingleTableMetadata
from trane.parsing.denormalize import (
    child_relationships,
    flatten_dataframes,
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
        # return _generate_possible_operations(
        #     table_meta=self.metadata
        # )


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
            original_columns = list(parent_table.columns)
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


#     check_target_entity(target_entity, relationships, list(dataframes.keys()))
#     relationship_order = child_relationships(target_entity, relationships)
#     relationship_order = reorder_relationships(target_entity, relationship_order)


#         parent_key = parent_table_name + "." + parent_key

#         flat = flatten_dataframes(parent_table, child_table, parent_key, child_key)
#         merged_dataframes[child_table_name] = (flat, original_to_new)
#         merged_dataframes[parent_table_name] = (flat, original_to_new)
#     # TODO: set primary key to be the index
#     # TODO: pass information to table meta (primary key, foreign keys)? maybe? technically relationships has this info
#     return merged_dataframes[target_entity][0]


# def _generate_possible_operations(
#     metadata,
#     exclude_columns,
#     aggregation_operations,
#     filter_operations,
# ):
#     if aggregation_operations is None:
#         aggregation_operations = get_aggregation_ops()
#     if filter_operations is None:
#         filter_operations = get_filter_ops()

#     valid_columns = list(metadata.keys())
#     if exclude_columns is None:
#         exclude_columns = []
#     elif exclude_columns and len(exclude_columns) > 0:
#         valid_columns = [col for col in valid_columns if col not in exclude_columns]

#     possible_operations = []
#     column_pairs = []
#     for filter_col, agg_col in itertools.product(
#         valid_columns,
#         valid_columns,
#     ):
#         column_pairs.append((filter_col, agg_col))

#     for agg_operation, filter_operation in itertools.product(
#         aggregation_operations,
#         filter_operations,
#     ):
#         for filter_col, agg_col in column_pairs:
#             # not ideal, what if there is more than 1 input type in the op
#             agg_op_input_type = convert_op_type(agg_operation.input_output_types[0][0])
#             filter_op_input_type = convert_op_type(
#                 filter_operation.input_output_types[0][0],
#             )
#             agg_instance = None
#             if (
#                 len(
#                     agg_operation.restricted_semantic_tags.intersection(
#                         metadata[agg_col].semantic_tags,
#                     ),
#                 )
#                 > 0
#             ):
#                 # if the agg operation is about to apply to a column that has restricted semantic tags
#                 continue
#             elif agg_op_input_type in ["None", None, ColumnSchema()]:
#                 agg_instance = agg_operation(None)
#             else:
#                 agg_instance = agg_operation(agg_col)
#             filter_instance = None
#             if (
#                 len(
#                     filter_operation.restricted_semantic_tags.intersection(
#                         metadata[filter_col].semantic_tags,
#                     ),
#                 )
#                 > 0
#             ):
#                 # if the agg operation is about to apply to a column that has restricted semantic tags
#                 continue
#             elif filter_op_input_type in ["None", None, ColumnSchema()]:
#                 filter_instance = filter_operation(None)
#             else:
#                 filter_instance = filter_operation(filter_col)
#             possible_operations.append((filter_instance, agg_instance))
#     # TODO: why are duplicate problems being generated
#     possible_operations = list(set(possible_operations))
#     return possible_operations
