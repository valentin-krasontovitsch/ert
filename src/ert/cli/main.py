#!/usr/bin/env python
import asyncio
import contextlib
import dataclasses
import logging
import os
import sys
import threading
import uuid
import warnings
from typing import Any

from ert._c_wrappers.enkf import EnKFMain, ErtConfig
from ert.cli import (
    ENSEMBLE_EXPERIMENT_MODE,
    ENSEMBLE_SMOOTHER_MODE,
    ES_MDA_MODE,
    ITERATIVE_ENSEMBLE_SMOOTHER_MODE,
    TEST_RUN_MODE,
    WORKFLOW_MODE,
)
from ert.cli.model_factory import create_model
from ert.cli.monitor import Monitor
from ert.cli.workflow import execute_workflow
from ert.ensemble_evaluator import EvaluatorServerConfig, EvaluatorTracker
from ert.libres_facade import LibresFacade
from ert.shared.feature_toggling import FeatureToggling
from ert.storage import StorageAccessor, open_storage


class ErtCliError(Exception):
    pass


class ErtTimeoutError(Exception):
    pass


def run_cli(args, _=None):
    ert_config = ErtConfig.from_file(args.config)
    try:
        with warnings.catch_warnings(record=True) as silenced_warnings:
            warnings.simplefilter("always")

            ert_config_new = ErtConfig.from_file(args.config, use_new_parser=True)

            for w in silenced_warnings:
                logging.info(f"Parser warning: {w.message}")

        if ert_config != ert_config_new:
            fields = dataclasses.fields(ert_config)
            difference = [
                f"{getattr(ert_config, field.name)} !="
                f" {getattr(ert_config_new, field.name)}"
                for field in fields
                if getattr(ert_config, field.name)
                != getattr(ert_config_new, field.name)
            ]
            logging.info(
                f"New parser gave different result.\n" f" Difference: {difference!r}"
            )
        else:
            logging.info("New parser gave equal result.")
    except Exception:
        logging.exception("The new parser failed")

    # Create logger inside function to make sure all handlers have been added to
    # the root-logger.
    logger = logging.getLogger(__name__)
    for job in ert_config.forward_model_list:
        logger.info("Config contains forward model job %s", job.name)

    for suggestion in ErtConfig.make_suggestion_list(args.config):
        print(f"Warning: {suggestion}")

    os.chdir(ert_config.config_path)
    ert = EnKFMain(ert_config)
    facade = LibresFacade(ert)
    if not facade.have_observations and args.mode not in [
        ENSEMBLE_EXPERIMENT_MODE,
        TEST_RUN_MODE,
        WORKFLOW_MODE,
    ]:
        raise ErtCliError(
            f"To run {args.mode}, observations are needed. \n"
            f"Please add an observation file to {args.config}. Example: \n"
            f"'OBS_CONFIG observation_file.txt'."
        )

    if not facade.have_smoother_parameters and args.mode in [
        ENSEMBLE_SMOOTHER_MODE,
        ES_MDA_MODE,
        ITERATIVE_ENSEMBLE_SMOOTHER_MODE,
    ]:
        raise ErtCliError(
            f"To run {args.mode}, GEN_KW, FIELD or SURFACE parameters are needed. \n"
            f"Please add to file {args.config}"
        )

    storage = open_storage(ert_config.ens_path, "w")

    if args.mode == WORKFLOW_MODE:
        execute_workflow(ert, storage, args.name)
        return

    evaluator_server_config = EvaluatorServerConfig(
        custom_port_range=args.port_range, custom_host="localhost"
    )
    experiment = storage.create_experiment(
        parameters=ert.ensembleConfig().parameter_configuration
    )

    # Note that asyncio.run should be called once in ert/shared/main.py
    if FeatureToggling.is_enabled("experiment-server"):
        asyncio.run(
            _run_cli_async(
                ert,
                storage,
                args,
                evaluator_server_config,
                experiment.id,
            ),
            debug=False,
        )
        return

    try:
        model = create_model(
            ert,
            storage,
            args,
            experiment.id,
        )
    except ValueError as e:
        raise ErtCliError(e) from e

    if model.check_if_runpath_exists():
        print("Warning: ERT is running in an existing runpath")
        logger.warning("ERT is running in an existing runpath")

    thread = threading.Thread(
        name="ert_cli_simulation_thread",
        target=model.start_simulations_thread,
        args=(evaluator_server_config,),
    )
    thread.start()

    tracker = EvaluatorTracker(
        model, ee_con_info=evaluator_server_config.get_connection_info()
    )

    with contextlib.ExitStack() as exit_stack:
        if args.disable_monitoring:
            out = exit_stack.enter_context(open(os.devnull, "w", encoding="utf-8"))
        else:
            out = sys.stderr
        monitor = Monitor(out=out, color_always=args.color_always)

        try:
            monitor.monitor(tracker.track())
        except (SystemExit, KeyboardInterrupt):
            print("\nKilling simulations...")
            tracker.request_termination()

    thread.join()
    storage.close()

    if model.hasRunFailed():
        raise ErtCliError(model.getFailMessage())


# pylint: disable=too-many-arguments
async def _run_cli_async(
    ert: EnKFMain,
    storage: StorageAccessor,
    args: Any,
    ee_config: EvaluatorServerConfig,
    experiment_id: uuid.UUID,
):
    # pylint: disable=import-outside-toplevel
    from ert.experiment_server import ExperimentServer

    experiment_server = ExperimentServer(ee_config)
    experiment_server.add_experiment(create_model(ert, storage, args, experiment_id))
    await experiment_server.run_experiment(experiment_id=experiment_id)
