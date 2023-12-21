import itertools
from typing import Dict, List

from trane.core.problem import Problem
from trane.ops.aggregation_ops import (
    AggregationOpBase,
)
from trane.ops.filter_ops import FilterOpBase
from trane.ops.transformation_ops import TransformationOpBase
from trane.ops.utils import get_aggregation_ops, get_filter_ops, get_transformation_ops
from trane.parsing.denormalize import (
    denormalize,
)
from trane.typing.ml_types import (
    Categorical,
    Integer,
    MLType,
    convert_op_type,
)


class ProblemGenerator:
    metadata = None
    target_table = None
    window_size = None
    entity_columns = []

    def __init__(
        self,
        metadata,
        window_size=None,
        target_table: str = None,
        entity_columns: List[str] = None,
    ):
        self.metadata = metadata
        self.window_size = window_size
        self.target_table = target_table
        self.entity_columns = entity_columns

    def generate(self, verbose=True):
        # denormalize and create single metadata table
        if self.metadata.get_metadata_type() == "single":
            single_metadata = self.metadata
        else:
            if self.target_table is None:
                raise ValueError(
                    "target_table must be specified for multi table metadata",
                )
            _, single_metadata = denormalize(
                metadata=self.metadata,
                target_table=self.target_table,
            )
            single_metadata.time_index = self.metadata.time_indices[self.target_table]
            single_metadata.original_multi_table_metadata = self.metadata
        possible_operations = _generate_possible_operations(
            ml_types=single_metadata.ml_types,
            primary_key=single_metadata.primary_key,
            time_index=single_metadata.time_index,
        )
        problems = []
        valid_entity_columns = self.entity_columns
        if self.entity_columns is None:
            # TODO: add logic to check entity_column
            valid_entity_columns = get_valid_entity_columns(single_metadata)
            # Force create with no entity column to generate problems "Predict X"
            valid_entity_columns.append(None)
        for entity_column in valid_entity_columns:
            for op_col_combo in possible_operations:
                filter_op, transform_op, agg_op = op_col_combo
                # Note: the order of the operations matters, the filter operation must be first
                operations = [filter_op, transform_op, agg_op]
                problem = Problem(
                    operations=operations,
                    metadata=single_metadata,
                    entity_column=entity_column,
                    window_size=self.window_size,
                )
                problem.target_table = self.target_table
                if problem.is_valid():
                    problems.append(problem)
        # sort by string representation
        problems = sorted(problems, key=lambda p: str(p))
        num_classification_problems = 0
        num_regression_problems = 0
        for problem in problems:
            if problem.is_classification():
                num_classification_problems += 1
            else:
                num_regression_problems += 1
        if verbose:
            print("=" * 50)
            print(f"Generated {len(problems)} total problems")
            print("-" * 50)
            print(f"Classification problems: {num_classification_problems}")
            print(f"Regression problems: {num_regression_problems}")
            print("=" * 50)
        return problems


def get_valid_entity_columns(metadata):
    entity_columns = []
    for col, ml_type in metadata.ml_types.items():
        if isinstance(ml_type, Categorical) or isinstance(ml_type, Integer):
            entity_columns.append(col)
    return entity_columns


def _generate_possible_operations(
    ml_types: Dict[str, MLType],
    primary_key: str = None,
    time_index: str = None,
    aggregation_operations: List[AggregationOpBase] = None,
    filter_operations: List[FilterOpBase] = None,
    transformation_operations: List[TransformationOpBase] = None,
):
    if aggregation_operations is None:
        aggregation_operations = get_aggregation_ops()
    if filter_operations is None:
        filter_operations = get_filter_ops()
    if transformation_operations is None:
        transformation_operations = get_transformation_ops()

    valid_columns = list(ml_types.keys())
    if primary_key is not None:
        valid_columns.remove(primary_key)
    if time_index is not None:
        valid_columns.remove(time_index)

    possible_operations = []
    column_combinations = []
    for filter_col, transform_col, agg_col in itertools.product(
        valid_columns,
        valid_columns,
        valid_columns,
    ):
        column_combinations.append((filter_col, transform_col, agg_col))

    for agg_operation, transform_operation, filter_operation in itertools.product(
        aggregation_operations,
        transformation_operations,
        filter_operations,
    ):
        for filter_col, transform_col, agg_col in column_combinations:
            # not ideal, what if there is more than 1 input type in the op
            agg_op_input_type = convert_op_type(agg_operation.input_output_types[0][0])
            transform_op_input_type = convert_op_type(
                transform_operation.input_output_types[0][0],
            )
            filter_op_input_type = convert_op_type(
                filter_operation.input_output_types[0][0],
            )
            agg_instance = None
            if (
                len(
                    agg_operation.restricted_tags.intersection(
                        ml_types[agg_col].get_tags(),
                    ),
                )
                > 0
            ):
                # if the agg operation is about to apply to a column that has restricted semantic tags
                continue
            elif agg_op_input_type in ["None", None, MLType, MLType()]:
                agg_instance = agg_operation(None)
            else:
                agg_instance = agg_operation(agg_col)
            transform_instance = None
            if (
                len(
                    transform_operation.restricted_tags.intersection(
                        ml_types[transform_col].get_tags(),
                    ),
                )
                > 0
            ):
                # if the agg operation is about to apply to a column that has restricted semantic tags
                continue
            elif transform_op_input_type in ["None", None, MLType, MLType()]:
                transform_instance = transform_operation(None)
            else:
                transform_instance = transform_operation(transform_col)
            filter_instance = None
            if (
                len(
                    filter_operation.restricted_tags.intersection(
                        ml_types[filter_col].get_tags(),
                    ),
                )
                > 0
            ):
                # if the agg operation is about to apply to a column that has restricted semantic tags
                continue
            elif filter_op_input_type in ["None", None, MLType, MLType()]:
                filter_instance = filter_operation(None)
            else:
                filter_instance = filter_operation(filter_col)
            possible_operations.append(
                (filter_instance, transform_instance, agg_instance),
            )
    # TODO: why are duplicate problems being generated
    unique_instances = {}
    for operation in possible_operations:
        filter_instance, transform_instance, agg_instance = operation
        if (filter_instance, agg_instance) not in unique_instances:
            unique_instances[(filter_instance, agg_instance)] = operation
    possible_operations = list(unique_instances.values())
    return possible_operations
