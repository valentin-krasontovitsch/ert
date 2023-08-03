import json
import os
import stat
from dataclasses import dataclass
from pathlib import Path
from unittest.mock import MagicMock, Mock

import pytest

import ert.ensemble_evaluator
from ert.config import ExtJob, QueueConfig, QueueDriverEnum
from ert.ensemble_evaluator.config import EvaluatorServerConfig
from ert.ensemble_evaluator.evaluator import EnsembleEvaluator
from ert.ensemble_evaluator.snapshot import SnapshotBuilder
from ert.load_status import LoadStatus

from .ensemble_evaluator_utils import TestEnsemble


@pytest.fixture
def snapshot():
    return (
        SnapshotBuilder()
        .add_step(step_id="0", status="Unknown")
        .add_job(
            step_id="0",
            job_id="0",
            index="0",
            name="job0",
            data={},
            status="Unknown",
        )
        .add_job(
            step_id="0",
            job_id="1",
            index="1",
            name="job1",
            data={},
            status="Unknown",
        )
        .add_job(
            step_id="0",
            job_id="2",
            index="2",
            name="job2",
            data={},
            status="Unknown",
        )
        .add_job(
            step_id="0",
            job_id="3",
            index="3",
            name="job3",
            data={},
            status="Unknown",
        )
        .build(["0", "1", "3", "4", "5", "9"], status="Unknown")
    )


@pytest.fixture
def queue_config():
    return QueueConfig(
        job_script="job_dispatch.py",
        max_submit=100,
        queue_system=QueueDriverEnum.LOCAL_DRIVER,
        queue_options=[("MAX_RUNNING", "50")],
    )


@pytest.fixture
def make_ensemble_builder(queue_config):
    def _make_ensemble_builder(tmpdir, num_reals, num_jobs, job_sleep=0):
        builder = ert.ensemble_evaluator.EnsembleBuilder()
        with tmpdir.as_cwd():
            ext_job_list = []
            for job_index in range(0, num_jobs):
                ext_job_config = Path(tmpdir) / f"EXT_JOB_{job_index}"
                with open(ext_job_config, "w", encoding="utf-8") as f:
                    f.write(f"EXECUTABLE ext_{job_index}.py\n")

                ext_job_exec = Path(tmpdir) / f"ext_{job_index}.py"
                with open(ext_job_exec, "w", encoding="utf-8") as f:
                    f.write(
                        "#!/usr/bin/env python\n"
                        "import time\n"
                        "\n"
                        'if __name__ == "__main__":\n'
                        f'    print("stdout from {job_index}")\n'
                        f"    time.sleep({job_sleep})\n"
                        f"    with open('status.txt', 'a', encoding='utf-8'): pass\n"
                    )
                mode = os.stat(ext_job_exec).st_mode
                mode |= stat.S_IXUSR | stat.S_IXGRP
                os.chmod(ext_job_exec, stat.S_IMODE(mode))

                ext_job_list.append(
                    ExtJob.from_config_file(
                        str(ext_job_config), name=f"ext_job_{job_index}"
                    )
                )

            @dataclass
            class RunArg:
                iens: int
                ensemble_storage = MagicMock()

            for iens in range(0, num_reals):
                run_path = Path(tmpdir / f"real_{iens}")
                os.mkdir(run_path)

                with open(run_path / "jobs.json", "w", encoding="utf-8") as f:
                    json.dump(
                        {
                            "jobList": [
                                _dump_ext_job(ext_job, index)
                                for index, ext_job in enumerate(ext_job_list)
                            ],
                        },
                        f,
                    )

                step = (
                    ert.ensemble_evaluator.StepBuilder()
                    .set_id("0")
                    .set_job_name("job dispatch")
                    .set_job_script("job_dispatch.py")
                    .set_max_runtime(10000)
                    .set_run_arg(Mock(iens=iens))
                    .set_done_callback(lambda _, _b: (LoadStatus.LOAD_SUCCESSFUL, ""))
                    .set_exit_callback(lambda _, _b: (LoadStatus.LOAD_FAILURE, ""))
                    # the first callback_argument is expected to be a run_arg
                    # from the run_arg, the queue wants to access the iens prop
                    .set_callback_arguments((RunArg(iens), None))
                    .set_run_path(run_path)
                    .set_num_cpu(1)
                    .set_name("dummy step")
                    .set_dummy_io()
                )

                for index, job in enumerate(ext_job_list):
                    step.add_job(
                        ert.ensemble_evaluator.LegacyJobBuilder()
                        .set_id(str(index))
                        .set_index(str(index))
                        .set_name(f"dummy job {index}")
                        .set_ext_job(job)
                    )

                builder.add_realization(
                    ert.ensemble_evaluator.RealizationBuilder()
                    .active(True)
                    .set_iens(iens)
                    .add_step(step)
                )

        analysis_config = Mock()
        analysis_config.get_stop_long_running = Mock(return_value=False)

        ecl_config = Mock()
        ecl_config.assert_restart = Mock()

        builder.set_legacy_dependencies(
            queue_config,
            analysis_config,
        )
        builder.set_id("0")
        return builder

    return _make_ensemble_builder


def _dump_ext_job(ext_job, index):
    return {
        "name": ext_job.name,
        "executable": ext_job.executable,
        "target_file": ext_job.target_file,
        "error_file": ext_job.error_file,
        "start_file": ext_job.start_file,
        "stdout": f"{index}.stdout",
        "stderr": f"{index}.stderr",
        "stdin": ext_job.stdin_file,
        "environment": None,
        "exec_env": {},
        "max_running": ext_job.max_running,
        "max_running_minutes": ext_job.max_running_minutes,
        "min_arg": ext_job.min_arg,
        "max_arg": ext_job.max_arg,
        "arg_types": ext_job.arg_types,
        "argList": ext_job.arglist,
    }


@pytest.fixture
def make_ee_config():
    def _ee_config(**kwargs):
        return EvaluatorServerConfig(custom_port_range=range(1024, 65535), **kwargs)

    return _ee_config


@pytest.fixture
def evaluator(make_ee_config):
    ensemble = TestEnsemble(0, 2, 1, 2, id_="0")
    ee = EnsembleEvaluator(
        ensemble,
        make_ee_config(),
        0,
    )
    yield ee
    ee.stop()
