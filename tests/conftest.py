import os
import resource
import shutil
from unittest.mock import MagicMock

import pkg_resources
import pytest
from hypothesis import HealthCheck, settings

from ert._c_wrappers.enkf import EnKFMain, ResConfig
from ert.services import Storage

from .utils import SOURCE_DIR

# CI runners produce unreliable test timings
# so too_slow healthcheck and deadline has to
# be supressed to avoid flaky behavior
settings.register_profile(
    "ci", max_examples=10, deadline=None, suppress_health_check=[HealthCheck.too_slow]
)


@pytest.fixture(scope="session", name="source_root")
def fixture_source_root():
    return SOURCE_DIR


@pytest.fixture(scope="class")
def class_source_root(request, source_root):
    request.cls.SOURCE_ROOT = source_root
    request.cls.TESTDATA_ROOT = source_root / "test-data"
    request.cls.SHARE_ROOT = pkg_resources.resource_filename("ert.shared", "share")
    yield


@pytest.fixture(autouse=True)
def env_save():
    exceptions = ["PYTEST_CURRENT_TEST", "KMP_DUPLICATE_LIB_OK", "KMP_INIT_AT_FORK"]
    environment_pre = [
        (key, val) for key, val in os.environ.items() if key not in exceptions
    ]
    yield
    environment_post = [
        (key, val) for key, val in os.environ.items() if key not in exceptions
    ]
    set_xor = set(environment_pre).symmetric_difference(set(environment_post))
    assert len(set_xor) == 0, f"Detected differences in environment: {set_xor}"


@pytest.fixture(scope="session", autouse=True)
def maximize_ulimits():
    """
    Bumps the soft-limit for max number of files up to its max-value
    since we know that the tests may open lots of files simultaneously.
    Resets to original when session ends.
    """
    limits = resource.getrlimit(resource.RLIMIT_NOFILE)
    resource.setrlimit(resource.RLIMIT_NOFILE, (limits[1], limits[1]))
    yield
    resource.setrlimit(resource.RLIMIT_NOFILE, limits)


@pytest.fixture(name="setup_case")
def fixture_setup_case(tmp_path, source_root, monkeypatch):
    def copy_case(path, config_file):
        shutil.copytree(os.path.join(source_root, "test-data", path), "test_data")
        monkeypatch.chdir(tmp_path / "test_data")
        return ResConfig(config_file)

    monkeypatch.chdir(tmp_path)
    yield copy_case


@pytest.fixture()
def poly_case(setup_case):
    return EnKFMain(setup_case("poly_example", "poly.ert"))


@pytest.fixture()
def snake_oil_case(setup_case):
    return EnKFMain(setup_case("snake_oil", "snake_oil.ert"))


@pytest.fixture()
def minimum_case(setup_case):
    return EnKFMain(setup_case("simple_config", "minimum_config"))


@pytest.fixture(name="copy_case")
def fixture_copy_case(tmp_path, source_root, monkeypatch):
    def _copy_case(path):
        shutil.copytree(os.path.join(source_root, "test-data", path), "test_data")
        monkeypatch.chdir(tmp_path / "test_data")

    monkeypatch.chdir(tmp_path)
    yield _copy_case


@pytest.fixture()
def copy_poly_case(copy_case):
    copy_case("poly_example")


@pytest.fixture()
def copy_snake_oil_case(copy_case):
    copy_case("snake_oil")


@pytest.fixture()
def copy_minimum_case(copy_case):
    copy_case("simple_config")


@pytest.fixture()
def use_tmpdir(tmp_path):
    cwd = os.getcwd()
    try:
        os.chdir(tmp_path)
        yield
    finally:
        os.chdir(cwd)


@pytest.fixture()
def mock_start_server(monkeypatch):
    start_server = MagicMock()
    monkeypatch.setattr(Storage, "start_server", start_server)
    yield start_server


@pytest.fixture()
def mock_connect(monkeypatch):
    connect = MagicMock()
    monkeypatch.setattr(Storage, "connect", connect)
    yield connect


def pytest_addoption(parser):
    parser.addoption(
        "--runslow", action="store_true", default=False, help="run slow tests"
    )
    parser.addoption(
        "--eclipse-simulator",
        action="store_true",
        default=False,
        help="Defaults to not running tests that require eclipse.",
    )


def pytest_collection_modifyitems(config, items):
    for item in items:
        fixtures = getattr(item, "fixturenames", ())
        if "qtbot" in fixtures or "qtmodeltester" in fixtures:
            item.add_marker("requires_window_manager")

    if config.getoption("--runslow"):
        # --runslow given in cli: do not skip slow tests
        skip_quick = pytest.mark.skip(
            reason="skipping quick performance tests on --runslow"
        )
        for item in items:
            if "quick_only" in item.keywords:
                item.add_marker(skip_quick)
            if item.get_closest_marker("requires_eclipse") and not config.getoption(
                "--eclipse_simulator"
            ):
                item.add_marker(pytest.mark.skip("Requires eclipse"))

    else:
        skip_slow = pytest.mark.skip(reason="need --runslow option to run")
        for item in items:
            if "slow" in item.keywords:
                item.add_marker(skip_slow)
            if item.get_closest_marker("requires_eclipse") and not config.getoption(
                "--eclipse-simulator"
            ):
                item.add_marker(pytest.mark.skip("Requires eclipse"))
