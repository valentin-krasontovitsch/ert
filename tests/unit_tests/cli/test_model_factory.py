from argparse import Namespace
from uuid import UUID

import pytest

from ert.cli import model_factory
from ert.libres_facade import LibresFacade
from ert.run_models import (
    EnsembleExperiment,
    EnsembleSmoother,
    IteratedEnsembleSmoother,
    MultipleDataAssimilation,
    SingleTestRun,
)

current_case = "default"
experiment_id = UUID(int=0)
realizations_range = "0-4,7,8"


@pytest.mark.parametrize(
    "target_case, format_mode, expected",
    [
        ("test", False, "test"),
        (None, False, "default_smoother_update"),
        (None, True, "default_%d"),
    ],
)
def test_target_case_name(target_case, expected, format_mode, poly_case):
    analysis_config_case_format = poly_case.analysisConfig().case_format
    assert (
        model_factory._target_case_name(
            analysis_config_case_format,
            current_case,
            target_case,
            format_mode=format_mode,
        )
        == expected
    )


def test_default_realizations(poly_case):
    facade = LibresFacade(poly_case)
    assert (
        model_factory._realizations(None, facade.get_ensemble_size())
        == [True] * facade.get_ensemble_size()
    )


def test_custom_realizations(poly_case):
    facade = LibresFacade(poly_case)
    ensemble_size = facade.get_ensemble_size()
    expected_mask = [False] * ensemble_size
    expected_mask[0] = True
    expected_mask[1] = True
    expected_mask[2] = True
    expected_mask[3] = True
    expected_mask[4] = True
    expected_mask[7] = True
    expected_mask[8] = True
    assert (
        model_factory._realizations(realizations_range, ensemble_size) == expected_mask
    )


def test_setup_single_test_run(poly_case, storage):
    ert = poly_case

    model = model_factory._setup_single_test_run(
        ert, storage, current_case, experiment_id
    )
    assert isinstance(model, SingleTestRun)
    assert len(model._simulation_arguments.keys()) == 3
    assert "active_realizations" in model._simulation_arguments


def test_setup_ensemble_experiment(poly_case, storage):
    ert = poly_case
    model = model_factory._setup_ensemble_experiment(
        ert,
        storage,
        iter_num=1,
        current_case=current_case,
        realizations_range=None,
        experiment_id=experiment_id,
    )
    assert isinstance(model, EnsembleExperiment)
    assert len(model._simulation_arguments.keys()) == 4
    assert "active_realizations" in model._simulation_arguments


def test_setup_ensemble_smoother(poly_case, storage):
    ert = poly_case
    analysis_config_case_format = poly_case.analysisConfig().case_format
    target_case = "test_case"

    model = model_factory._setup_ensemble_smoother(
        ert,
        storage,
        current_case,
        target_case,
        realizations_range,
        experiment_id,
        analysis_config_case_format,
    )
    assert isinstance(model, EnsembleSmoother)
    assert len(model._simulation_arguments.keys()) == 5
    assert "active_realizations" in model._simulation_arguments
    assert "target_case" in model._simulation_arguments
    assert "analysis_module" in model._simulation_arguments


def test_setup_multiple_data_assimilation(poly_case, storage):
    ert = poly_case
    analysis_config_case_format = poly_case.analysisConfig().case_format
    weights = "6,4,2"
    target_case = "test_case_%d"

    model = model_factory._setup_multiple_data_assimilation(
        ert,
        storage,
        realizations_range,
        current_case,
        target_case,
        weights,
        experiment_id,
        analysis_config_case_format,
        restart_case=current_case,
    )
    assert isinstance(model, MultipleDataAssimilation)
    assert len(model._simulation_arguments.keys()) == 8
    assert "active_realizations" in model._simulation_arguments
    assert "target_case" in model._simulation_arguments
    assert "analysis_module" in model._simulation_arguments
    assert "weights" in model._simulation_arguments


def test_setup_iterative_ensemble_smoother(poly_case, storage):
    ert = poly_case
    analysis_config_case_format = poly_case.analysisConfig().case_format
    args = Namespace(
        realizations="0-4,7,8",
        current_case="default",
        target_case="test_case_%d",
        num_iterations="10",
    )

    model = model_factory._setup_iterative_ensemble_smoother(
        ert,
        storage,
        args,
        UUID(int=0),
        analysis_config_case_format,
    )
    assert isinstance(model, IteratedEnsembleSmoother)
    assert len(model._simulation_arguments.keys()) == 6
    assert "active_realizations" in model._simulation_arguments
    assert "target_case" in model._simulation_arguments
    assert "analysis_module" in model._simulation_arguments
    assert "num_iterations" in model._simulation_arguments
    assert LibresFacade(ert).get_number_of_iterations() == 4
