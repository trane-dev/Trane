from pandas.api.types import (
    is_bool_dtype,
    is_categorical_dtype,
    is_datetime64_any_dtype,
    is_float_dtype,
    is_integer_dtype,
    is_string_dtype,
)

from trane.typing.inference_functions import (
    boolean_func,
    categorical_func,
    datetime_func,
    double_func,
    integer_func,
    natural_language_func,
)


class LogicalTypeMetaClass(type):
    def __repr__(cls):
        return cls.__name__


class LogicalType(object, metaclass=LogicalTypeMetaClass):
    dtype = None
    standard_tags = set()
    is_valid_dtype = None

    def __eq__(self, other, deep=False):
        return isinstance(other, self.__class__)

    def __str__(self):
        return str(self.__class__)

    def transform(self, series):
        return series.astype(self.dtype)

    @staticmethod
    def inference_func(series):
        raise NotImplementedError


class Boolean(LogicalType):
    dtype = "boolean[pyarrow]"
    is_valid_dtype = is_bool_dtype

    @staticmethod
    def inference_func(series):
        return boolean_func(series)


class Categorical(LogicalType):
    dtype = "category"
    standard_tags = {"category"}
    is_valid_dtype = is_categorical_dtype

    @staticmethod
    def inference_func(series):
        return categorical_func(series)


class Datetime(LogicalType):
    dtype = "datetime64[ns]"
    is_valid_dtype = is_datetime64_any_dtype

    def __init__(self, datetime_format=None, timezone=None):
        self.datetime_format = datetime_format
        self.timezone = timezone

    @staticmethod
    def inference_func(series):
        return datetime_func(series)


class Double(LogicalType):
    dtype = "float64[pyarrow]"
    standard_tags = {"numeric"}
    is_valid_dtype = is_float_dtype

    @staticmethod
    def inference_func(series):
        return double_func(series)


class Integer(LogicalType):
    dtype = "int64[pyarrow]"
    standard_tags = {"numeric"}
    is_valid_dtype = is_integer_dtype

    @staticmethod
    def inference_func(series):
        return integer_func(series)


class NaturalLanguage(LogicalType):
    dtype = "string[pyarrow]"
    is_valid_dtype = is_string_dtype

    @staticmethod
    def inference_func(series):
        return natural_language_func(series)


class Ordinal(LogicalType):
    dtype = "category"
    standard_tags = {"category"}
    is_valid_dtype = is_categorical_dtype

    def __init__(self, order=None):
        self.order = order


class PersonFullName(LogicalType):
    dtype = "string[pyarrow]"
    is_valid_dtype = is_string_dtype


class URL(LogicalType):
    dtype = "string[pyarrow]"
    is_valid_dtype = is_string_dtype


class EmailAddress(LogicalType):
    dtype = "string[pyarrow]"
    is_valid_dtype = is_string_dtype


class PostalCode(LogicalType):
    dtype = "string[pyarrow]"
    is_valid_dtype = is_string_dtype


class Filepath(LogicalType):
    dtype = "string[pyarrow]"
    is_valid_dtype = is_string_dtype


class LatLong(LogicalType):
    dtype = "object"
    is_valid_dtype = is_string_dtype


class IPAddress(LogicalType):
    dtype = "string[pyarrow]"
    is_valid_dtype = is_string_dtype


class PhoneNumber(LogicalType):
    dtype = "string[pyarrow]"
    is_valid_dtype = is_string_dtype


class Unknown(LogicalType):
    dtype = "string[pyarrow]"
    is_valid_dtype = is_string_dtype
