class LogicalTypeMetaClass(type):
    def __repr__(cls):
        return cls.__name__


class LogicalType(object, metaclass=LogicalTypeMetaClass):
    def __eq__(self, other, deep=False):
        return isinstance(other, self.__class__)

    def __str__(self):
        return str(self.__class__)


class Boolean(LogicalType):
    pass


class Categorical(LogicalType):
    pass


class Datetime(LogicalType):
    pass


class Double(LogicalType):
    pass


class Integer(LogicalType):
    pass


class Ordinal(LogicalType):
    pass


class PostalCode(LogicalType):
    pass


ALL_LOGICAL_TYPES = [
    Boolean,
    Categorical,
    Datetime,
    Double,
    Integer,
    Ordinal,
]
