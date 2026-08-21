import pytest


def pytest_addoption(parser):
    parser.addoption(
        "--datastore-root",
        action="store",
        default=None,
        help="Shared Metaflow datastore root for -m remote tests, e.g. s3://bucket/metaflow.",
    )


@pytest.fixture
def datastore_root(request):
    root = request.config.getoption("--datastore-root")
    if not root:
        pytest.skip("--datastore-root is required for remote tests")
    return root
