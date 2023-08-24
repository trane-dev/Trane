from trane.core.problem import Problem
from trane.core.problem_generator import ProblemGenerator
from trane.metadata import SingleTableMetadata
from trane.utils.testing_utils import generate_mock_data


def test_problem_generator_single_table():
    dataframes, ml_types, _, primary_key, time_index = generate_mock_data(
        tables=["products"],
    )

    # 1. User creates single table metadata
    metadata = SingleTableMetadata(
        ml_types=ml_types,
        primary_key="id",
        time_index=time_index,
    )

    # 2. User specifies their window size (for each data slice)
    window_size = "2d"
    problem_generator = ProblemGenerator(
        metadata=metadata,
        window_size=window_size,
    )
    problems = problem_generator.generate()
    for p in problems:
        assert isinstance(p, Problem)

    # 3. Generate target values for each problem
    for p in problems:
        # TODO: how should threshold be generated?

        # 1. User specifies threshold
        # p.create_target_values(dataframes, threshold=0.5)

        # TODO: Add better threshold generation
        p.create_target_values(dataframes)


#         print(p)
#         print(label_times)


# def test_problem_generator_multi_table():
#     SingleTableMetadata(
#         ml_types=ml_types,
#         primary_key={"order"},
#         time_index=time_index,
#     )
