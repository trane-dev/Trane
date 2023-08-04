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
    tags = set()

    def __init__(self, tags=None):
        if tags is None:
            tags = set()
        self.tags = tags

    def __eq__(self, other, deep=False):
        return isinstance(other, self.__class__)

    def __str__(self):
        return str(self.__class__)

    def transform(self, series):
        return series.astype(self.dtype)

    @staticmethod
    def inference_func(series):
        raise NotImplementedError


class Boolean(MLType):
    dtype = "boolean[pyarrow]"

    @staticmethod
    def inference_func(series):
        return boolean_func(series)


class Categorical(MLType):
    dtype = "category"
    tags = {"category"}

    @staticmethod
    def inference_func(series):
        return categorical_func(series)


class Datetime(MLType):
    dtype = "datetime64[ns]"

    def __init__(self, datetime_format=None, timezone=None, **kwargs):
        self.datetime_format = datetime_format
        self.timezone = timezone
        super().__init__(**kwargs)

    @staticmethod
    def inference_func(series):
        return datetime_func(series)


class Double(MLType):
    dtype = "float64[pyarrow]"
    tags = {"numeric"}

    @staticmethod
    def inference_func(series):
        return double_func(series)


class Integer(MLType):
    dtype = "int64[pyarrow]"
    tags = {"numeric"}

    @staticmethod
    def inference_func(series):
        return integer_func(series)


class NaturalLanguage(MLType):
    dtype = "string[pyarrow]"

    @staticmethod
    def inference_func(series):
        return natural_language_func(series)


class Ordinal(MLType):
    dtype = "category"
    tags = {"category"}

    def __init__(self, order=None):
        self.order = order


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
