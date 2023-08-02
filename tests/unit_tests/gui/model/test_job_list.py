import datetime

import pytest
from dateutil import tz
from PyQt5.QtCore import QModelIndex
from pytestqt.qt_compat import qt_api

from ert.ensemble_evaluator import identifiers as ids
from ert.ensemble_evaluator.snapshot import Job, PartialSnapshot
from ert.ensemble_evaluator.state import (
    JOB_STATE_FAILURE,
    JOB_STATE_RUNNING,
    JOB_STATE_START,
)
from ert.gui.model.job_list import JobListProxyModel
from ert.gui.model.node import NodeType
from ert.gui.model.snapshot import COLUMNS, DURATION, SnapshotModel

from .gui_models_utils import partial_snapshot


def _id_to_col(identifier):
    for col, fields in enumerate(COLUMNS[NodeType.STEP]):
        if fields[1] == identifier:
            return col
    raise ValueError(f"{identifier} not a column in {COLUMNS}")


def test_using_qt_model_tester(qtmodeltester, full_snapshot):
    partial = partial_snapshot(full_snapshot)
    source_model = SnapshotModel()

    model = JobListProxyModel(None, 0, 0, 0, 0)
    model.setSourceModel(source_model)

    reporting_mode = qt_api.QtTest.QAbstractItemModelTester.FailureReportingMode.Warning
    tester = qt_api.QtTest.QAbstractItemModelTester(  # noqa, prevent GC pylint: disable=unused-variable
        model, reporting_mode
    )

    source_model._add_snapshot(SnapshotModel.prerender(full_snapshot), 0)
    source_model._add_snapshot(SnapshotModel.prerender(full_snapshot), 1)

    source_model._add_partial_snapshot(SnapshotModel.prerender(partial), 0)
    source_model._add_partial_snapshot(SnapshotModel.prerender(partial), 1)

    qtmodeltester.check(model, force_py=True)


@pytest.mark.requires_window_manager
def test_changes(full_snapshot):
    source_model = SnapshotModel()

    model = JobListProxyModel(None, 0, 0, 0, 0)
    model.setSourceModel(source_model)

    reporting_mode = qt_api.QtTest.QAbstractItemModelTester.FailureReportingMode.Warning
    tester = qt_api.QtTest.QAbstractItemModelTester(  # noqa, prevent GC pylint: disable=unused-variable
        model, reporting_mode
    )

    source_model._add_snapshot(SnapshotModel.prerender(full_snapshot), 0)
    assert (
        model.index(0, _id_to_col(ids.STATUS), QModelIndex()).data() == JOB_STATE_START
    )

    partial = PartialSnapshot(full_snapshot)
    start_time = datetime.datetime(year=2020, month=10, day=27, hour=12)
    end_time = datetime.datetime(year=2020, month=10, day=28, hour=13)
    partial.update_job(
        "0",
        "0",
        "0",
        job=Job(
            status=JOB_STATE_FAILURE,
            start_time=start_time,
            end_time=end_time,
        ),
    )
    source_model._add_partial_snapshot(SnapshotModel.prerender(partial), 0)
    assert (
        model.index(0, _id_to_col(DURATION), QModelIndex()).data() == "1 day, 1:00:00"
    )
    assert (
        model.index(0, _id_to_col(ids.STATUS), QModelIndex()).data()
        == JOB_STATE_FAILURE
    )


@pytest.mark.requires_window_manager
@pytest.mark.parametrize("timezone", [(None), (tz.gettz("UTC"))])
def test_duration(timezone, full_snapshot):
    source_model = SnapshotModel()

    model = JobListProxyModel(None, 0, 0, 0, 0)
    model.setSourceModel(source_model)

    reporting_mode = qt_api.QtTest.QAbstractItemModelTester.FailureReportingMode.Warning
    tester = qt_api.QtTest.QAbstractItemModelTester(  # noqa, prevent GC pylint: disable=unused-variable
        model, reporting_mode
    )

    source_model._add_snapshot(SnapshotModel.prerender(full_snapshot), 0)
    assert (
        model.index(0, _id_to_col(ids.STATUS), QModelIndex()).data() == JOB_STATE_START
    )

    partial = PartialSnapshot(full_snapshot)
    start_time = datetime.datetime.now(timezone) - datetime.timedelta(
        days=1, hours=1, minutes=12, seconds=11
    )
    partial.update_job(
        "0",
        "0",
        "2",
        job=Job(
            status=JOB_STATE_RUNNING,
            start_time=start_time,
        ),
    )
    source_model._add_partial_snapshot(SnapshotModel.prerender(partial), 0)
    assert (
        model.index(2, _id_to_col(DURATION), QModelIndex())
        .data()
        .startswith("1 day, 1:12:1")
    )


@pytest.mark.requires_window_manager
def test_no_cross_talk(full_snapshot):
    source_model = SnapshotModel()

    model = JobListProxyModel(None, 0, 0, 0, 0)
    model.setSourceModel(source_model)

    reporting_mode = qt_api.QtTest.QAbstractItemModelTester.FailureReportingMode.Warning
    qt_api.QtTest.QAbstractItemModelTester(model, reporting_mode)  # noqa, prevent GC

    source_model._add_snapshot(SnapshotModel.prerender(full_snapshot), 0)
    source_model._add_snapshot(SnapshotModel.prerender(full_snapshot), 1)

    # Test that changes to iter=1 does not bleed into iter=0
    partial = PartialSnapshot(full_snapshot)
    partial.update_job("0", "0", "0", job=Job(status=JOB_STATE_FAILURE))
    source_model._add_partial_snapshot(SnapshotModel.prerender(partial), 1)
    assert (
        model.index(0, _id_to_col(ids.STATUS), QModelIndex()).data() == JOB_STATE_START
    )

    model.set_step(1, 0, 0, 0)
    assert (
        model.index(0, _id_to_col(ids.STATUS), QModelIndex()).data()
        == JOB_STATE_FAILURE
    )
