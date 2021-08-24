import pytest


@pytest.fixture
def env(monkeypatch):
    monkeypatch.setenv("ERT_STORAGE_DATABASE_URL", "sqlite://")
    monkeypatch.setenv("ERT_STORAGE_NO_TOKEN", "yup")


@pytest.fixture
def ert_storage_app(env):
    from ert_storage.app import app

    return app


@pytest.fixture
def dark_storage_app(env):
    from ert_shared.dark_storage.app import app

    return app


def test_openapi(ert_storage_app, dark_storage_app):
    """
    Test that the openapi.json of Dark Storage is identical to ERT Storage
    """
    expect = ert_storage_app.openapi()
    actual = dark_storage_app.openapi()

    # Remove textual data (descriptions and such) from ERT Storage's API.
    def _remove_text(data):
        if isinstance(data, dict):
            return {
                key: _remove_text(val)
                for key, val in data.items()
                if key not in ("description", "examples")
            }
        return data

    assert _remove_text(expect) == _remove_text(actual)