class LogicalType(object):
    pass


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
