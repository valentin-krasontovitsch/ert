from unittest.mock import MagicMock
import sys
from ert_shared import main
import ert_logging
import pytest


def test_main_logging(monkeypatch, caplog):
    parser_mock = MagicMock()
    parser_mock.func.side_effect = ValueError
    monkeypatch.setattr(ert_logging, "logging", MagicMock())
    monkeypatch.setattr(main, "ert_parser", MagicMock(return_value=parser_mock))
    monkeypatch.setattr(main, "start_ert_server", MagicMock())
    monkeypatch.setattr(main, "ErtPluginContext", MagicMock())
    monkeypatch.setattr(sys, "argv", ["ert", "test_run", "config.ert"])
    with pytest.raises(SystemExit, match="ert crashed unexpectedly"):
        main.main()
    assert "ert crashed unexpectedly\nTraceback" in caplog.text