from datetime import datetime

import pandas as pd

from trane.ops import AggregationOpBase
from trane.ops.filter_ops import FilterOpBase
from trane.typing.column_schema import ColumnSchema
from trane.typing.logical_types import (
    ALL_LOGICAL_TYPES,
    Categorical,
    Datetime,
    Double,
    Integer,
)

TYPE_MAPPING = {
    "category": ColumnSchema(semantic_tags={"category"}),
    "index": ColumnSchema(semantic_tags={"index"}),
    None: ColumnSchema(),
    "numeric": ColumnSchema(semantic_tags={"numeric"}),
    "Double": ColumnSchema(logical_type=Double, semantic_tags={"numeric"}),
    "Integer": ColumnSchema(logical_type=Integer, semantic_tags={"numeric"}),
}


def clean_date(date):
    if isinstance(date, str):
        return pd.Timestamp(datetime.strptime(date, "%Y-%m-%d"))
    return date


def _parse_table_meta(table_meta):
    str_to_logical_type = {ltype.__name__.lower(): ltype for ltype in ALL_LOGICAL_TYPES}
    parsed_schema = {}
    for col, schema in table_meta.items():
        if isinstance(schema, str):
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
            # operation applies to all columns
            if op_input_type is None:
                continue
            if isinstance(op_input_type, str):
                op_input_type = TYPE_MAPPING[op_input_type]
            if isinstance(op_output_type, str):
                op_output_type = TYPE_MAPPING[op_output_type]
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
    assert "index" in entity_col_type.semantic_tags

    time_col_type = table_meta[time_col]
    assert time_col_type.logical_type == Datetime
