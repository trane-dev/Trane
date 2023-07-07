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

    @staticmethod
    def inference_func(series):
        return categorical_func(series)


class Datetime(LogicalType):
    dtype = "datetime64[ns]"

    @staticmethod
    def inference_func(series):
        return datetime_func(series)


class Double(LogicalType):
    dtype = "float64"

    @staticmethod
    def inference_func(series):
        return double_func(series)


class Integer(LogicalType):
    dtype = "int64"

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


class Unknown(LogicalType):
    dtype = "string"


ALL_LOGICAL_TYPES = [
    Boolean,
    Categorical,
    Datetime,
    Double,
    Integer,
    Ordinal,
]
