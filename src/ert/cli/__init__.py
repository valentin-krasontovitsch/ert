ENSEMBLE_SMOOTHER_MODE = "ensemble_smoother"
ENSEMBLE_EXPERIMENT_MODE = "ensemble_experiment"
ITERATIVE_ENSEMBLE_SMOOTHER_MODE = "iterative_ensemble_smoother"
ES_MDA_MODE = "es_mda"
TEST_RUN_MODE = "test_run"
WORKFLOW_MODE = "workflow"

MODULE_MODE = {
    "EnsembleSmoother": ENSEMBLE_SMOOTHER_MODE,
    "EnsembleExperiment": ENSEMBLE_EXPERIMENT_MODE,
    "IteratedEnsembleSmoother": ITERATIVE_ENSEMBLE_SMOOTHER_MODE,
    "MultipleDataAssimilation": ES_MDA_MODE,
    "SingleTestRun": TEST_RUN_MODE,
}

SIMULATION_MODES = [
    ENSEMBLE_SMOOTHER_MODE,
    ENSEMBLE_EXPERIMENT_MODE,
    ITERATIVE_ENSEMBLE_SMOOTHER_MODE,
    ES_MDA_MODE,
    TEST_RUN_MODE,
]
