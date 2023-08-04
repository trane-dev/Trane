from trane.typing.ml_types import MLType


class Metadata:
    def __init__(self):
        self.relationships = []
        self.ml_types = {}
        self.indexes = {}
        self.time_indexes = {}

    def set_time_index(self, table, time_index):
        self.time_indexes[table] = time_index

    def set_index(self, table, index):
        self.indexes[table] = index

    def set_type(self, table, column, ml_type, tags=set()):
        ml_type = check_ml_type(ml_type)
        ml_type.tags = tags
        self.ml_types[table][column] = ml_type

    def add_relationships(self, relationships):
        if not isinstance(relationships, list):
            relationships = [relationships]
        check_relationships(relationships)
        self.relationships.append(relationships)


def check_relationships(relationships):
    for relationship in relationships:
        if not isinstance(relationship, tuple) or len(relationship) != 4:
            raise ValueError(
                "Relationship must be a tuple (parent_table_name, parent_join_key, child_table_name, child_join_key)",
            )


def check_ml_type(ml_type):
    if isinstance(ml_type, MLType):
        return ml_type

    str_to_logical_type = {
        ltype.__name__.lower(): ltype for ltype in MLType.__subclasses__()
    }

    if ml_type.lower() not in str_to_logical_type:
        raise ValueError("ML Type is Invalid")
    return str_to_logical_type[ml_type.lower()]
