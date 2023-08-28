from __future__ import annotations

import logging
import warnings
from typing import TYPE_CHECKING, Any, List, Optional, overload
from uuid import UUID

from ert.cli import (
    ENSEMBLE_EXPERIMENT_MODE,
    ENSEMBLE_SMOOTHER_MODE,
    ES_MDA_MODE,
    ITERATIVE_ENSEMBLE_SMOOTHER_MODE,
    TEST_RUN_MODE,
)
from ert.config import ConfigWarning
from ert.enkf_main import EnKFMain
from ert.run_models import (
    EnsembleExperiment,
    EnsembleSmoother,
    IteratedEnsembleSmoother,
    MultipleDataAssimilation,
    SingleTestRun,
)
from ert.validation import ActiveRange

if TYPE_CHECKING:
    from ert.run_models import BaseRunModel
    from ert.storage import StorageAccessor


def create_model(
    ert: EnKFMain,
    storage: StorageAccessor,
    mode: str,
    args: Any,
    experiment_id: UUID,
) -> BaseRunModel:
    logger = logging.getLogger(__name__)
    logger.info(
        "Initiating experiment",
        extra={
            "mode": mode,
            "ensemble_size": ert.getEnsembleSize(),
        },
    )

    if mode == TEST_RUN_MODE:
        return _setup_single_test_run(ert, storage, args.current_case, experiment_id)

    if mode == ENSEMBLE_EXPERIMENT_MODE:
        return _setup_ensemble_experiment(
            ert,
            storage,
            int(args.iter_num),
            args.current_case,
            args.realizations,
            experiment_id,
        )

    analysis_config_case_format = ert.analysisConfig().case_format

    if mode == ENSEMBLE_SMOOTHER_MODE:
        return _setup_ensemble_smoother(
            ert,
            storage,
            args.current_case,
            args.target_case,
            args.realizations,
            experiment_id,
            analysis_config_case_format,
        )

    if mode == ES_MDA_MODE:
        return _setup_multiple_data_assimilation(
            ert,
            storage,
            args.realizations,
            args.current_case,
            args.target_case,
            args.weights,
            experiment_id,
            analysis_config_case_format,
            args.restart_case,
        )

    assert mode == ITERATIVE_ENSEMBLE_SMOOTHER_MODE
    return _setup_iterative_ensemble_smoother(
        ert,
        storage,
        args.current_case,
        args.target_case,
        args.realizations,
        experiment_id,
        analysis_config_case_format,
    )


def _setup_single_test_run(
    ert: EnKFMain, storage: StorageAccessor, current_case: str, experiment_id: UUID
) -> SingleTestRun:
    simulations_argument = {
        "active_realizations": [True],
        "current_case": current_case,
        "simulation_mode": "Single test run",
    }
    model = SingleTestRun(simulations_argument, ert, storage, experiment_id)
    return model


def _setup_ensemble_experiment(
    ert: EnKFMain,
    storage: StorageAccessor,
    iter_num: int,
    current_case: str,
    realizations_range: str,
    experiment_id: UUID,
) -> EnsembleExperiment:
    min_realizations_count = ert.analysisConfig().minimum_required_realizations
    active_realizations = _realizations(realizations_range, ert.getEnsembleSize())
    active_realizations_count = len(
        [i for i in range(len(active_realizations)) if active_realizations[i]]
    )

    if active_realizations_count < min_realizations_count:
        ert.analysisConfig().minimum_required_realizations = active_realizations_count
        warnings.warn(
            f"Due to active_realizations {active_realizations_count} is lower than "
            f"MIN_REALIZATIONS {min_realizations_count}, MIN_REALIZATIONS has been "
            f"set to match active_realizations.",
            category=ConfigWarning,
        )

    simulations_argument = {
        "active_realizations": active_realizations,
        "iter_num": int(iter_num),
        "current_case": current_case,
        "simulation_mode": "Ensemble experiment",
    }
    model = EnsembleExperiment(
        simulations_argument, ert, storage, ert.get_queue_config(), experiment_id
    )

    return model


def _setup_ensemble_smoother(
    ert: EnKFMain,
    storage: StorageAccessor,
    current_case: str,
    target_case: str,
    realizations_range: str,
    experiment_id: UUID,
    analysis_config_case_format: Optional[str],
) -> EnsembleSmoother:
    simulations_argument = {
        "active_realizations": _realizations(realizations_range, ert.getEnsembleSize()),
        "current_case": current_case,
        "target_case": _target_case_name(
            analysis_config_case_format,
            current_case,
            target_case,
            format_mode=False,
        ),
        "analysis_module": "STD_ENKF",
        "simulation_mode": "Ensemble smoother",
    }
    model = EnsembleSmoother(
        simulations_argument,
        ert,
        storage,
        ert.get_queue_config(),
        experiment_id,
    )
    return model


@overload
def _setup_multiple_data_assimilation(
    ert: EnKFMain,
    storage: StorageAccessor,
    realizations_range: str,
    current_case: str,
    target_case: str,
    weights: str,
    experiment_id: UUID,
    analysis_config_case_format: Optional[str],
    restart_case: str,
) -> MultipleDataAssimilation:
    ...


@overload
def _setup_multiple_data_assimilation(
    ert: EnKFMain,
    storage: StorageAccessor,
    realizations_range: str,
    current_case: str,
    target_case: str,
    weights: str,
    experiment_id: UUID,
    analysis_config_case_format: Optional[str],
    *,
    restart_run: bool,
    prior_ensemble: str,
) -> MultipleDataAssimilation:
    ...


def _setup_multiple_data_assimilation(
    ert: EnKFMain,
    storage: StorageAccessor,
    realizations_range: str,
    current_case: str,
    target_case: str,
    weights: str,
    experiment_id: UUID,
    analysis_config_case_format: Optional[str],
    restart_case: Optional[str] = None,
    restart_run: Optional[bool] = None,
    prior_ensemble: Optional[str] = None,
) -> MultipleDataAssimilation:
    # Because the configuration of the CLI is different from the gui, we
    # have a different way to get the restart information.
    if restart_case is not None:
        restart_run = restart_case is not None
        prior_ensemble = restart_case

    simulations_argument = {
        "active_realizations": _realizations(realizations_range, ert.getEnsembleSize()),
        "target_case": _target_case_name(
            analysis_config_case_format,
            current_case,
            target_case,
            format_mode=True,
        ),
        "analysis_module": "STD_ENKF",
        "weights": weights,
        "num_iterations": len(weights),
        "restart_run": restart_run,
        "prior_ensemble": prior_ensemble,
        "simulation_mode": "Multiple data assimilation",
    }
    model = MultipleDataAssimilation(
        simulations_argument,
        ert,
        storage,
        ert.get_queue_config(),
        experiment_id,
    )
    return model


def _setup_iterative_ensemble_smoother(
    ert: EnKFMain,
    storage: StorageAccessor,
    current_case: str,
    target_case: str,
    realizations_range: str,
    id_: UUID,
    analysis_config_case_format: Optional[str],
) -> IteratedEnsembleSmoother:
    simulations_argument = {
        "active_realizations": _realizations(realizations_range, ert.getEnsembleSize()),
        "current_case": current_case,
        "target_case": _target_case_name(
            analysis_config_case_format,
            current_case,
            target_case,
            format_mode=True,
        ),
        "analysis_module": "IES_ENKF",
        "num_iterations": ert.analysisConfig().num_iterations,
        "simulation_mode": "Iterative ensemble smoother",
    }
    model = IteratedEnsembleSmoother(
        simulations_argument, ert, storage, ert.get_queue_config(), id_
    )
    return model


def _realizations(realizations_range: str, ensemble_size: int) -> List[bool]:
    if realizations_range is None:
        return [True] * ensemble_size
    return ActiveRange(rangestring=realizations_range, length=ensemble_size).mask


def _target_case_name(
    analysis_config_case_format: Optional[str],
    current_case: str,
    target_case: Optional[str],
    format_mode: bool = False,
) -> str:
    if target_case is not None:
        return target_case

    if not format_mode:
        return f"{current_case}_smoother_update"

    if analysis_config_case_format:
        return analysis_config_case_format

    return f"{current_case}_%d"
