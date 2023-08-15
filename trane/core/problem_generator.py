from trane.metadata import BaseMetadata
from trane.parsing.denormalize import (
    denormalize,
)


class ProblemGenerator:
    metadata = None
    target_table = None
    window_size = None
    entity_columns = []
    problem_type = ["classification", "regression"]

    def __init__(
        self,
        metadata,
        window_size,
        target_table,
        entity_columns,
        problem_type,
    ):
        if not issubclass(metadata, BaseMetadata):
            raise ValueError("metadata is not a valid type")
        metadata.check_if_table_exists(target_table)

        self.metadata = metadata
        self.window_size = window_size
        self.target_table = target_table
        self.entity_columns = entity_columns
        self.problem_type = problem_type

    def generate(self):
        # denormalize and create single metadata table
        denormalize(
            metadata=self.metadata,
            target_table=self.target_table,
        )
