import itertools
from datetime import datetime
from typing import Dict, List

import pandas as pd

from trane.ops.aggregation_ops import (
    AggregationOpBase,
)
from trane.ops.filter_ops import FilterOpBase
from trane.ops.utils import get_aggregation_ops, get_filter_ops
from trane.typing.column_schema import ColumnSchema
from trane.typing.logical_types import (
    Boolean,
    Categorical,
    Datetime,
    Double,
    Integer,
    LogicalType,
)

TYPE_MAPPING = {
    "category": ColumnSchema(semantic_tags={"category"}),
    "primary_key": ColumnSchema(semantic_tags={"primary_key"}),
    "foreign_key": ColumnSchema(semantic_tags={"foreign_key"}),
    "None": ColumnSchema(),
    "numeric": ColumnSchema(semantic_tags={"numeric"}),
    "Categorical": ColumnSchema(logical_type=Categorical, semantic_tags={"category"}),
    "Double": ColumnSchema(logical_type=Double, semantic_tags={"numeric"}),
    "Integer": ColumnSchema(logical_type=Integer, semantic_tags={"numeric"}),
    "Boolean": ColumnSchema(logical_type=Boolean),
    "Datetime": ColumnSchema(logical_type=Datetime),
}


def clean_date(date):
    if isinstance(date, str):
        return pd.Timestamp(datetime.strptime(date, "%Y-%m-%d"))
    return date


def _parse_table_meta(table_meta):
    str_to_logical_type = {
        ltype.__name__.lower(): ltype for ltype in LogicalType.__subclasses__()
    }
    parsed_schema = {}
    for col, schema in table_meta.items():
        if isinstance(schema, str):
            if schema.lower() in TYPE_MAPPING:
                parsed_schema[col] = TYPE_MAPPING[schema.lower()]
            else:
                parsed_schema[col] = ColumnSchema(
                    logical_type=str_to_logical_type[schema.lower()],
                )
        elif isinstance(schema, tuple):
            logical_type = None
            semantic_tags = None
            if schema[0]:
                logical_type = str_to_logical_type[schema[0].lower()]
            if schema[1]:
                semantic_tags = schema[1]
            parsed_schema[col] = ColumnSchema(
                logical_type=logical_type,
                semantic_tags=semantic_tags,
            )
        elif isinstance(schema, ColumnSchema):
            parsed_schema[col] = schema
    return parsed_schema


def convert_op_type(op_type):
    if isinstance(op_type, str):
        return TYPE_MAPPING[op_type]
    return op_type


def _check_operations_valid(
    operations,
    table_meta,
):
    if not isinstance(operations[0], FilterOpBase):
        raise ValueError
    if not isinstance(operations[1], AggregationOpBase):
        raise ValueError
    for op in operations:
        input_output_types = op.input_output_types
        for op_input_type, op_output_type in input_output_types:
            op_input_type = convert_op_type(op_input_type)
            op_output_type = convert_op_type(op_output_type)

            # operation applies to all columns
            if op_input_type == ColumnSchema() and op_input_type.semantic_tags == set():
                if (
                    op_output_type == ColumnSchema()
                    and op_input_type.semantic_tags == set()
                ):
                    # op doesn't modify the column's type
                    pass
                else:
                    # update the column's type (to indicate the operation has taken place)
                    table_meta[op.column_name] = op_output_type
                continue

            # check the operation is valid for the column
            column_logical_type = table_meta[op.column_name].logical_type
            column_semantic_tags = table_meta[op.column_name].semantic_tags

            op_input_logical_type = op_input_type.logical_type
            op_input_semantic_tags = op_input_type.semantic_tags

            if op_input_logical_type and column_logical_type != op_input_logical_type:
                return False, {}
            if not column_semantic_tags.issubset(op_input_semantic_tags):
                return False, {}
            # update the column's type (to indicate the operation has taken place)
            table_meta[op.column_name] = op_output_type
    return True, table_meta


def _generate_possible_operations(
    table_meta: Dict[str, ColumnSchema],
    exclude_columns: List[str] = None,
    aggregation_operations: List[AggregationOpBase] = None,
    filter_operations: List[FilterOpBase] = None,
):
    if aggregation_operations is None:
        aggregation_operations = get_aggregation_ops()
    if filter_operations is None:
        filter_operations = get_filter_ops()

    valid_columns = list(table_meta.keys())
    if exclude_columns is None:
        exclude_columns = []
    elif exclude_columns and len(exclude_columns) > 0:
        valid_columns = [col for col in valid_columns if col not in exclude_columns]

    possible_operations = []
    column_pairs = []
    for filter_col, agg_col in itertools.product(
        valid_columns,
        valid_columns,
    ):
        column_pairs.append((filter_col, agg_col))

    for agg_operation, filter_operation in itertools.product(
        aggregation_operations,
        filter_operations,
    ):
        for filter_col, agg_col in column_pairs:
            # not ideal, what if there is more than 1 input type in the op
            agg_op_input_type = convert_op_type(agg_operation.input_output_types[0][0])
            filter_op_input_type = convert_op_type(
                filter_operation.input_output_types[0][0],
            )
            agg_instance = None
            if (
                len(
                    agg_operation.restricted_semantic_tags.intersection(
                        table_meta[agg_col].semantic_tags,
                    ),
                )
                > 0
            ):
                # if the agg operation is about to apply to a column that has restricted semantic tags
                continue
            elif agg_op_input_type in ["None", None, ColumnSchema()]:
                agg_instance = agg_operation(None)
            else:
                agg_instance = agg_operation(agg_col)
            filter_instance = None
            if (
                len(
                    filter_operation.restricted_semantic_tags.intersection(
                        table_meta[filter_col].semantic_tags,
                    ),
                )
                > 0
            ):
                # if the agg operation is about to apply to a column that has restricted semantic tags
                continue
            elif filter_op_input_type in ["None", None, ColumnSchema()]:
                filter_instance = filter_operation(None)
            else:
                filter_instance = filter_operation(filter_col)
            possible_operations.append((filter_instance, agg_instance))
    # TODO: why are duplicate problems being generated
    possible_operations = list(set(possible_operations))
    return possible_operations


def get_semantic_tags(filter_op: FilterOpBase):
    """
    Extract the semantic tags from the filter operation, looking at the input_output_types.

    Return:
        valid_semantic_tags(set(str)): a set of semantic tags that the filter operation can be applied to.
    """
    valid_semantic_tags = set()
    for op_input_type, _ in filter_op.input_output_types:
        if isinstance(op_input_type, str):
            op_input_type = TYPE_MAPPING[op_input_type]
        valid_semantic_tags.update(op_input_type.semantic_tags)
    return valid_semantic_tags


def check_table_meta(table_meta, entity_col, time_col):
    assert isinstance(table_meta, dict)
    assert isinstance(entity_col, str)
    assert isinstance(time_col, str)

    for col, col_type in table_meta.items():
        assert isinstance(col, str)
        assert isinstance(col_type, ColumnSchema)

    entity_col_type = table_meta[entity_col]
    assert entity_col_type.logical_type in [Integer, Categorical]
    assert "primary_key" in entity_col_type.semantic_tags

    time_col_type = table_meta[time_col]
    assert time_col_type.logical_type == Datetime
