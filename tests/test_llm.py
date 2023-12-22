import os

import pytest

from trane import MultiTableMetadata, ProblemGenerator
from trane.llm import analyze
from trane.utils.testing_utils import generate_mock_data


@pytest.fixture
def metadata():
    tables = ["customers", "sessions", "products", "logs"]
    (
        _,
        ml_types,
        relationships,
        primary_keys,
        time_indices,
    ) = generate_mock_data(
        tables=tables,
    )
    metadata = MultiTableMetadata(
        ml_types=ml_types,
        primary_keys=primary_keys,
        relationships=relationships,
        time_indices=time_indices,
    )
    return metadata


@pytest.fixture
def problems(metadata):
    problem_generator = ProblemGenerator(
        metadata=metadata,
        entity_column=["product_id"],
        target_table="products",
    )
    problems = problem_generator.generate()
    return problems


@pytest.mark.parametrize(
    "model",
    [
        ("gpt-4-1106-preview"),
    ],
)
@pytest.mark.skipif(
    "OPENAI_API_KEY" not in os.environ,
    reason="OPEN AI API KEY not found in environment variables",
)
def test_open_ai(problems, model):
    instructions = "determine 5 most relevant problems about products"
    context = "a fake dataset of ecommerce data"
    relevant_problems = analyze(
        problems=problems,
        instructions=instructions,
        context=context,
        model=model,
    )
    relevant_problems
