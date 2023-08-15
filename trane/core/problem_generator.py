import itertools
from typing import Dict, List

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
    Boolean,
    Categorical,
    Datetime,
    Double,
    Integer,
    MLType,
)

TYPE_MAPPING = {
    # "category": Categorical,
    # "primary_key": ColumnSchema(semantic_tags={"primary_key"}),
    # "foreign_key": ColumnSchema(semantic_tags={"foreign_key"}),
    # "numeric": ColumnSchema(semantic_tags={"numeric"}),
    "None": MLType,
    "Categorical": Categorical(tags={"category"}),
    "Double": Double(tags={"numeric"}),
    "Integer": Integer(tags={"numeric"}),
    "Boolean": Boolean,
    "Datetime": Datetime,
}


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
        target_table: str = None,
        entity_columns: List[str] = None,
        problem_type: str = None,
    ):
        self.metadata = metadata
        self.window_size = window_size
        self.target_table = target_table
        self.entity_columns = entity_columns
        self.problem_type = problem_type

    def generate(self):
        # denormalize and create single metadata table
        if self.metadata.get_metadata_type() == "single":
            single_metadata = self.metadata
        else:
            _, single_metadata = denormalize(
                metadata=self.metadata,
                target_table=self.target_table,
            )
        possible_operations = _generate_possible_operations(
            ml_types=single_metadata.ml_types,
            primary_key=single_metadata.primary_key,
        )

        all_attempts = 0
        for op_col_combo in possible_operations:
            all_attempts += 1
            filter_op_obj, transform_op_obj, agg_op_obj = op_col_combo

            # Note: the order of the operations matters, the filter operation must be first
            operations = [filter_op_obj, transform_op_obj, agg_op_obj]
            print(operations)

            # problem = Problem(
            #     operations=operations,
            #     entity_col=self.entity_col,
            #     time_col=self.time_col,
            #     table_meta=self.table_meta,
            #     cutoff_strategy=self.cutoff_strategy,
            # )
            # filter_op = problem.operations[0]
            # if isinstance(filter_op, AllFilterOp):
            #     # the filter operation does not require a threshold
            #     problems.append(problem)
            #     success_attempts += 1
            # else:
            #     yielded_thresholds = self._threshold_recommend(filter_op, df)
            #     for threshold in yielded_thresholds:
            #         final_problem = copy.deepcopy(problem)
            #         final_problem.operations[0].set_parameters(
            #             threshold=threshold,
            #         )
            #         problems.append(final_problem)
            #         success_attempts += 1


def _generate_possible_operations(
    ml_types: Dict[str, MLType],
    primary_key: str = None,
    exclude_columns: List[str] = None,
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
                    agg_operation.restricted_semantic_tags.intersection(
                        ml_types[agg_col]().get_tags(),
                    ),
                )
                > 0
            ):
                # if the agg operation is about to apply to a column that has restricted semantic tags
                continue
            elif agg_op_input_type in ["None", None, MLType]:
                agg_instance = agg_operation(None)
            else:
                agg_instance = agg_operation(agg_col)
            transform_instance = None
            if (
                len(
                    transform_operation.restricted_semantic_tags.intersection(
                        ml_types[transform_col]().get_tags(),
                    ),
                )
                > 0
            ):
                # if the agg operation is about to apply to a column that has restricted semantic tags
                continue
            elif transform_op_input_type in ["None", None, MLType]:
                transform_instance = transform_operation(None)
            else:
                transform_instance = transform_operation(transform_col)
            filter_instance = None
            if (
                len(
                    filter_operation.restricted_semantic_tags.intersection(
                        ml_types[filter_col]().get_tags(),
                    ),
                )
                > 0
            ):
                # if the agg operation is about to apply to a column that has restricted semantic tags
                continue
            elif filter_op_input_type in ["None", None, MLType]:
                filter_instance = filter_operation(None)
            else:
                filter_instance = filter_operation(filter_col)
            possible_operations.append(
                (filter_instance, transform_instance, agg_instance),
            )
    # TODO: why are duplicate problems being generated
    possible_operations = list(set(possible_operations))
    return possible_operations


def convert_op_type(op_type):
    if isinstance(op_type, str) and op_type in TYPE_MAPPING:
        return TYPE_MAPPING[op_type]
    return op_type
