import pytest


def pytest_addoption(parser):
    parser.addoption(
        "--datastore-root",
        action="store",
        default=None,
        help=(
            "Datastore root for -m remote tests, e.g. s3://bucket/metaflow. Optional: "
            "without it the flow falls back to Flyte's own object store."
        ),
    )


@pytest.fixture
def datastore_root(request):
    """The configured datastore root, or ``None`` to exercise the default."""
    return request.config.getoption("--datastore-root")
