from collections import defaultdict

from trane.typing.inference import infer_metadata
from trane.typing.ml_types import Datetime, MLType


class BaseMetadata:
    def __init__(self):
        raise NotImplementedError

    def set_primary_key(self):
        raise NotImplementedError

    def set_time_index(self):
        raise NotImplementedError

    def get_ml_type(self):
        raise NotImplementedError


class SingleTableMetadata(BaseMetadata):
    ml_types = defaultdict(dict)
    primary_key = None
    time_index = None

    def __init__(self, ml_types: dict, primary_key: str = None, time_index: str = None):
        self.ml_types = _parse_ml_types(ml_types, type_=self.get_metadata_type())
        if primary_key:
            self.set_primary_key(primary_key)
        if time_index:
            self.set_time_index(time_index)

    def set_primary_key(self, primary_key):
        if primary_key not in self.ml_types:
            raise ValueError("Index does not exist in ml_types")
        elif (
            self.primary_key
            and "primary_key" in self.ml_types[self.primary_key].get_tags()
        ):
            self.ml_types[self.primary_key].remove_tag("primary_key")
        self.primary_key = primary_key
        self.ml_types[primary_key].tags.add("primary_key")

    def reset_primary_key(self):
        if self.primary_key:
            self.ml_types[self.primary_key].remove_tag("primary_key")
        self.primary_key = None

    def set_time_index(self, time_index):
        if time_index not in self.ml_types:
            raise ValueError("Time index does not exist in ml_types")
        elif self.time_index and self.ml_types[self.time_index].has_tag("time_index"):
            self.ml_types[self.time_index].remove_tag("time_index")

        if time_index and not isinstance(self.get_ml_type(time_index), Datetime):
            raise ValueError("Time index must be of type Datetime")
        self.time_index = time_index
        self.ml_types[time_index].tags.add("time_index")

    def reset_time_index(self):
        if self.time_index:
            self.ml_types[self.time_index].remove_tag("time_index")
        self.time_index = None

    def set_type(self, column, ml_type, tags=set()):
        ml_type = check_ml_type(ml_type)
        ml_type.tags = tags
        self.ml_types[column] = ml_type

    def get_ml_type(self, column):
        return self.ml_types[column]

    @staticmethod
    def from_data(dataframe):
        ml_types = infer_metadata(dataframe)
        return SingleTableMetadata(ml_types)

    @staticmethod
    def get_metadata_type():
        return "single"


class MultiTableMetadata(BaseMetadata):
    ml_types = defaultdict(dict)

    def __init__(
        self,
        ml_types: dict,
        primary_keys=None,
        time_primary_keys=None,
        relationships: list = None,
    ):
        self.ml_types = _parse_ml_types(ml_types, type_=self.get_metadata_type())
        self.primary_keys = {}
        if primary_keys:
            self.set_primary_keys(primary_keys)
        self.time_primary_keys = {}
        if time_primary_keys:
            self.set_time_primary_keys(time_primary_keys)
        self.relationships = []
        if relationships:
            self.add_relationships(relationships)

    @staticmethod
    def get_metadata_type():
        return "multi"

    def set_time_primary_keys(self, time_primary_keys):
        for table, time_index_column in time_primary_keys.items():
            self.set_time_index(table, time_index_column)

    def set_time_index(self, table, column):
        self.check_if_table_exists(table)
        if not isinstance(self.get_ml_type(table, column), Datetime):
            raise ValueError("Time index must be of type Datetime")
        self.time_primary_keys[table] = column
        self.ml_types[table][column] = Datetime()

    def set_primary_keys(self, primary_keys):
        for table, index_column in primary_keys.items():
            self.set_primary_key(table, index_column)

    def set_primary_key(self, table, column):
        self.check_if_table_exists(table)
        if column not in self.ml_types[table]:
            raise ValueError("Index does not exist in ml_types")
        self.primary_keys[table] = column
        self.ml_types[table][column].tags.add("primary_key")

    def reset_primary_key(self, table):
        self.check_if_table_exists(table)
        if self.primary_keys:
            primary_key = self.primary_keys[table]
            self.ml_types[table][primary_key].remove_tag("primary_key")
        self.primary_keys.pop(table)

    def add_table(self, table, ml_types):
        if table in self.ml_types:
            raise ValueError("Table already exists")
        self.ml_types[table] = _parse_ml_types(
            ml_types,
            type_="single",
        )

    def get_ml_type(self, table, column):
        self.check_if_table_exists(table)
        return self.ml_types[table][column]

    def set_type(self, table, column, ml_type):
        ml_type = check_ml_type(ml_type)
        self.ml_types[table][column] = ml_type

    def add_relationships(self, relationships):
        if not isinstance(relationships, list):
            relationships = [relationships]
        self.check_relationships(relationships)
        for rel in relationships:
            if rel not in self.relationships:
                self.relationships.append(rel)

    def check_if_table_exists(self, table):
        if table not in self.ml_types:
            raise ValueError(f"Table: {table} does not exist")

    def remove_table(self, table):
        self.ml_types.pop(table, None)
        self.primary_keys.pop(table, None)
        self.time_primary_keys.pop(table, None)

    def remove_relationship(self, relationships):
        if not isinstance(relationships, list):
            relationships = [relationships]
        for rel in self.relationships:
            self.relationships.remove(rel)

    def clear_relationships(self):
        self.relationships = []

    def check_relationships(self, relationships):
        for rel in relationships:
            if not isinstance(rel, tuple) or len(rel) != 4:
                raise ValueError(
                    "Relationship must be a tuple (parent_table_name, parent_join_key, child_table_name, child_join_key)",
                )
            parent_table_name, parent_key, child_table_name, child_key = rel
            if parent_table_name not in self.ml_types:
                raise ValueError(
                    f"{parent_table_name} not in ml_types",
                )
            if parent_key not in self.ml_types[parent_table_name]:
                raise ValueError(
                    f"{parent_key} not in ml_types[{parent_table_name}]",
                )
            if child_table_name not in self.ml_types:
                raise ValueError(
                    f"{child_key} not in table",
                )
            if child_key not in self.ml_types[child_table_name]:
                raise ValueError(
                    f"{child_key} not in ml_types[{child_table_name}]",
                )


def check_ml_type(ml_type):
    if isinstance(ml_type, str):
        ml_type = ml_type.lower()
        str_to_ml_type = _str_to_ml_types()
        if ml_type.lower() not in str_to_ml_type.keys():
            raise ValueError("ML Type is Invalid")
        return str_to_ml_type[ml_type.lower()]()
    if isinstance(ml_type, MLType) or issubclass(ml_type, MLType):
        return ml_type
    return ml_type


def _parse_ml_types(ml_types, type_="single"):
    new_ml_types = defaultdict(dict)
    if type_ == "single":
        for col, ml_type in ml_types.items():
            new_ml_types[col] = check_ml_type(ml_type)
    else:
        for table, columns in ml_types.items():
            for col, ml_type in columns.items():
                new_ml_types[table][col] = check_ml_type(ml_type)
    return new_ml_types


def _str_to_ml_types():
    str_to_logical_type = {
        ltype.__name__.lower(): ltype for ltype in MLType.__subclasses__()
    }
    return str_to_logical_type
