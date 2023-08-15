from trane.core.problem import Problem
from trane.core.problem_generator import ProblemGenerator
from trane.metadata import SingleTableMetadata
from trane.utils.testing_utils import generate_mock_data


def test_problem_generator_single_table():
    dataframes, ml_types, relationships, primary_keys = generate_mock_data(
        tables=["products"],
    )
    ml_types = ml_types["products"]
    primary_key = primary_keys["products"]
    metadata = SingleTableMetadata(ml_types=ml_types, primary_key=primary_key)
    window_size = "2d"
    problem_generator = ProblemGenerator(metadata=metadata, window_size=window_size)
    problems = problem_generator.generate()
    for p in problems:
        assert isinstance(p, Problem)
