import pandas as pd

from trane.typing.inference_functions import (
    boolean_func,
    categorical_func,
    datetime_func,
    double_func,
    integer_func,
    natural_language_func,
)


class MLTypeMetaClass(type):
    def __repr__(cls):
        return cls.__name__


class MLType(object, metaclass=MLTypeMetaClass):
    dtype = None
    mandatory_tags = set()

    def __init__(self, tags=None):
        self.tags = set()
        if tags is not None:
            self.add_tags(tags)

    def __eq__(self, other, deep=False):
        if not isinstance(other, self.__class__):
            return False
        if self.dtype and other.dtype and self.dtype != other.dtype:
            return False
        if (
            self.get_tags() != other.get_tags()
            or not self.get_tags().issubset(other.get_tags())
            or not other.get_tags().issubset(self.get_tags())
            or len(self.get_tags()) != len(other.get_tags())
        ):
            return False
        return True

    def __str__(self):
        return str(self.__class__)

    def __repr__(self):
        return self.__str__()

    def transform(self, series: pd.Series):
        return series.astype(self.dtype)

    @staticmethod
    def inference_func(series):
        raise NotImplementedError

    def get_tags(self):
        return self.tags | self.mandatory_tags

    def add_tags(self, new_tags):
        if isinstance(new_tags, set) or isinstance(new_tags, list):
            self.tags.update(new_tags)
        elif isinstance(new_tags, str):
            self.tags.add(new_tags)
        else:
            self.tags.update(set(new_tags))

    def remove_tag(self, tag):
        self.tags.remove(tag)

    def has_tag(self, tag):
        return tag in self.tags

    @property
    def is_numeric(self):
        return False

    @property
    def is_boolean(self):
        return False

    @property
    def is_datetime(self):
        False

    @property
    def is_categorical(self):
        return False


class Boolean(MLType):
    dtype = "boolean[pyarrow]"

    @staticmethod
    def inference_func(series):
        return boolean_func(series)

    @property
    def is_boolean(self):
        return True


class Categorical(MLType):
    dtype = "category"
    mandatory_tags = {"category"}

    @staticmethod
    def inference_func(series):
        return categorical_func(series)

    @property
    def is_categorical(self):
        return True


class Datetime(MLType):
    dtype = "datetime64[ns]"

    @staticmethod
    def inference_func(series):
        return datetime_func(series)

    @property
    def is_datetime(self):
        return True


class Double(MLType):
    dtype = "float64[pyarrow]"
    mandatory_tags = {"numeric"}

    @staticmethod
    def inference_func(series):
        return double_func(series)

    @property
    def is_numeric(self):
        return True


class Integer(MLType):
    dtype = "int64[pyarrow]"
    mandatory_tags = {"numeric"}

    @staticmethod
    def inference_func(series):
        return integer_func(series)

    @property
    def is_numeric(self):
        return True


class NaturalLanguage(MLType):
    dtype = "string[pyarrow]"

    @staticmethod
    def inference_func(series):
        return natural_language_func(series)


class Ordinal(MLType):
    dtype = "category"
    mandatory_tags = {"category"}


class PersonFullName(MLType):
    dtype = "string[pyarrow]"


class URL(MLType):
    dtype = "string[pyarrow]"


class EmailAddress(MLType):
    dtype = "string[pyarrow]"


class PostalCode(MLType):
    dtype = "string[pyarrow]"


class Filepath(MLType):
    dtype = "string[pyarrow]"


class LatLong(MLType):
    dtype = "object"


class IPAddress(MLType):
    dtype = "string[pyarrow]"


class PhoneNumber(MLType):
    dtype = "string[pyarrow]"


class Unknown(MLType):
    dtype = "string[pyarrow]"


TYPE_MAPPING = {
    "None": MLType,
    "category": (MLType, {"category"}),
    "numeric": (MLType, {"numeric"}),
    "Categorical": Categorical,
    "Double": Double,
    "Integer": Integer,
    "Boolean": Boolean,
    "Datetime": Datetime,
}


def convert_op_type(op_type):
    if isinstance(op_type, str):
        if isinstance(TYPE_MAPPING[op_type], tuple):
            ml_type = TYPE_MAPPING[op_type][0]
            tags = TYPE_MAPPING[op_type][1]
            return ml_type(tags=tags)
        return TYPE_MAPPING[op_type]()
    return op_type
