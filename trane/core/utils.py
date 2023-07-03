from trane.column_schema import ColumnSchema
from trane.logical_types import ALL_LOGICAL_TYPES


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
        else:
            raise TypeError(f"Invalid schema type for column '{col}'")
    return parsed_schema
