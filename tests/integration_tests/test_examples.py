from trane.core.problem_generator import ProblemGenerator
from trane.datasets.load_functions import load_airbnb
from trane.metadata import SingleTableMetadata
from trane.typing.ml_types import Categorical


def test_airbnb_reviews():
    data = load_airbnb()
    ml_types = {
        "listing_id": "Categorical",
        "id": "Categorical",
        "date": "Datetime",
        "reviewer_id": "Categorical",
        "location": Categorical(tags="primary_key"),
        "rating": "Categorical",
    }
    assert data["id"].is_unique
    metadata = SingleTableMetadata(
        ml_types=ml_types,
        primary_key="id",
        time_index="date",
    )
    window_size = "1m"
    problem_generator = ProblemGenerator(
        metadata=metadata,
        window_size=window_size,
    )
    problems = problem_generator.generate()
    for p in problems:
        p.create_target_values(data)
