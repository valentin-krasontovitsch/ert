from functools import partial

from cwrap import BaseCClass

from res import ResPrototype
from res.enkf.enkf_state import EnKFState
from res.enkf.ert_run_context import ErtRunContext
from res.job_queue import JobQueueManager, RunStatusType


class EnkfSimulationRunner(BaseCClass):
    TYPE_NAME = "enkf_simulation_runner"

    _create_run_path = ResPrototype(
        "void enkf_main_create_run_path(enkf_simulation_runner, ert_run_context)"
    )

    def __init__(self, enkf_main):
        assert isinstance(enkf_main, BaseCClass)
        # enkf_main should be an EnKFMain, get the _RealEnKFMain object
        real_enkf_main = enkf_main.parent()
        super().__init__(
            real_enkf_main.from_param(real_enkf_main).value,
            parent=real_enkf_main,
            is_reference=True,
        )

    def _enkf_main(self):
        return self.parent()

    def runSimpleStep(self, job_queue, run_context):
        """@rtype: int"""
        #### run simplestep ####
        self._enkf_main().initRun(run_context)

        if run_context.get_step():
            ecl_config = self._enkf_main().ecl_config.assert_restart()

        #### start queue ####
        self.start_queue(run_context, job_queue)

        #### deactivate failed realizations ####
        totalOk = 0
        totalFailed = 0
        for i in range(len(run_context)):
            if run_context.is_active(i):
                run_arg = run_context[i]
                if (
                    run_arg.run_status == RunStatusType.JOB_LOAD_FAILURE
                    or run_arg.run_status == RunStatusType.JOB_RUN_FAILURE
                ):
                    run_context.deactivate_realization(i)
                    totalFailed += 1
                else:
                    totalOk += 1

        run_context.get_sim_fs().fsync()

        if totalFailed == 0:
            print("All {} active jobs complete and data loaded.".format(totalOk))
        else:
            print("{} active job(s) failed.".format(totalFailed))

        return totalOk

    def createRunPath(self, run_context: ErtRunContext):
        self._create_run_path(run_context)

    def runEnsembleExperiment(self, job_queue, run_context):
        """@rtype: int"""
        return self.runSimpleStep(job_queue, run_context)

    @staticmethod
    def runWorkflows(runtime, ert):
        """:type res.enkf.enum.HookRuntimeEnum"""
        hook_manager = ert.getHookManager()
        hook_manager.runWorkflows(runtime, ert)

    def start_queue(self, run_context, job_queue):
        max_runtime = self._enkf_main().analysisConfig().get_max_runtime()
        if max_runtime == 0:
            max_runtime = None

        done_callback_function = EnKFState.forward_model_ok_callback
        exit_callback_function = EnKFState.forward_model_exit_callback

        # submit jobs
        for i in range(len(run_context)):
            if not run_context.is_active(i):
                continue
            run_arg = run_context[i]
            job_queue.add_job_from_run_arg(
                run_arg,
                self._enkf_main().resConfig(),
                max_runtime,
                done_callback_function,
                exit_callback_function,
            )

        job_queue.submit_complete()
        queue_evaluators = None
        if (
            self._enkf_main().analysisConfig().get_stop_long_running()
            and self._enkf_main().analysisConfig().minimum_required_realizations > 0
        ):
            queue_evaluators = [
                partial(
                    job_queue.stop_long_running_jobs,
                    self._enkf_main().analysisConfig().minimum_required_realizations,
                )
            ]

        jqm = JobQueueManager(job_queue, queue_evaluators)
        jqm.execute_queue()
