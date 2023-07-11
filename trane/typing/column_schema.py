from inspect import isclass

from trane.typing.logical_types import Boolean, Datetime


class ColumnSchema(object):
    def __init__(
        self,
        logical_type=None,
        semantic_tags=None,
    ):
        """Create ColumnSchema
        Args:
            logical_type (LogicalType, optional): The column's LogicalType.
            semantic_tags (str, list, set, optional): The semantic tag(s) specified for the column.
        """
        if isclass(logical_type):
            self.logical_type = logical_type()
        semantic_tags = self._parse_column_tags(semantic_tags)
        self.logical_type = logical_type
        self.semantic_tags = semantic_tags

    def __eq__(self, other, deep=True):
        if not isinstance(other, ColumnSchema):
            return False
        if (
            self.logical_type
            and other.logical_type
            and self.logical_type != other.logical_type
        ):
            return False
        if (
            self.semantic_tags != other.semantic_tags
            or self.semantic_tags.issubset(other.semantic_tags) is False
            or other.semantic_tags.issubset(self.semantic_tags) is False
            or len(self.semantic_tags) != len(other.semantic_tags)
        ):
            return False
        return True

    def __repr__(self):
        msg = "<ColumnSchema"
        if self.logical_type is not None:
            msg += " (Logical Type = {})".format(self.logical_type)
        if self.semantic_tags:
            msg += " (Semantic Tags = {})".format(sorted(list(self.semantic_tags)))
        msg += ">"
        return msg

    def _parse_column_tags(self, semantic_tags):
        if not semantic_tags:
            return set()

        if isinstance(semantic_tags, str):
            return {semantic_tags}

        if isinstance(semantic_tags, list):
            semantic_tags = set(semantic_tags)

        return semantic_tags

    @property
    def is_numeric(self):
        """Whether the ColumnSchema is numeric in nature"""
        return self.logical_type is not None and "numeric" in self.semantic_tags

    @property
    def is_categorical(self):
        """Whether the ColumnSchema is categorical in nature"""
        return self.logical_type is not None and "category" in self.semantic_tags

    @property
    def is_datetime(self):
        """Whether the ColumnSchema is a Datetime column"""
        return issubclass(self.logical_type, Datetime)

    @property
    def is_boolean(self):
        """Whether the ColumnSchema is a Boolean column"""
        return issubclass(self.logical_type, Boolean)
