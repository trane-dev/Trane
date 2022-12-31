import pytest


def pytest_addoption(parser):
    parser.addoption("--sample", action="store", default=None)


@pytest.fixture
def sample(request):
    return request.config.getoption("--sample")
