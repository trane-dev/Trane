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

    def __eq__(self, other, deep=False):
        return isinstance(other, self.__class__)

    def __str__(self):
        return str(self.__class__)

    @staticmethod
    def inference_func(series):
        raise NotImplementedError

    def transform(self, series):
        return series.astype(self.dtype)


class Boolean(LogicalType):
    dtype = "bool"

    @staticmethod
    def inference_func(series):
        return boolean_func(series)


class Categorical(LogicalType):
    dtype = "category"
    standard_tags = {"category"}

    @staticmethod
    def inference_func(series):
        return categorical_func(series)


class Datetime(LogicalType):
    dtype = "datetime64[ns]"

    def __init__(self, datetime_format=None, timezone=None):
        self.datetime_format = datetime_format
        self.timezone = timezone

    @staticmethod
    def inference_func(series):
        return datetime_func(series)


class Double(LogicalType):
    dtype = "float64"
    standard_tags = {"numeric"}

    @staticmethod
    def inference_func(series):
        return double_func(series)


class Integer(LogicalType):
    dtype = "int64"
    standard_tags = {"numeric"}

    @staticmethod
    def inference_func(series):
        return integer_func(series)


class NaturalLanguage(LogicalType):
    dtype = "string"

    @staticmethod
    def inference_func(series):
        return natural_language_func(series)


class Ordinal(LogicalType):
    dtype = "category"
    standard_tags = {"category"}

    def __init__(self, order=None):
        self.order = order


class PersonFullName(LogicalType):
    dtype = "string"


class URL(LogicalType):
    dtype = "string"


class EmailAddress(LogicalType):
    dtype = "string"


class PostalCode(LogicalType):
    dtype = "string"


class Filepath(LogicalType):
    dtype = "string"


class LatLong(LogicalType):
    dtype = "object"


class IPAddress(LogicalType):
    dtype = "string"


class PhoneNumber(LogicalType):
    dtype = "string"


class Unknown(LogicalType):
    dtype = "string"
