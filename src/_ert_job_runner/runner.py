import os

from _ert_job_runner.job import Job
from _ert_job_runner.reporting.message import Finish, Init


class JobRunner:
    def __init__(self, jobs_data):
        self.simulation_id = jobs_data.get("run_id")
        self.experiment_id = jobs_data.get("experiment_id")
        self.ens_id = jobs_data.get("ens_id")
        self.real_id = jobs_data.get("real_id")
        self.step_id = jobs_data.get("step_id")
        self.ert_pid = jobs_data.get("ert_pid")
        self.global_environment = jobs_data.get("global_environment")
        job_data_list = jobs_data["jobList"]

        if self.simulation_id is not None:
            os.environ["ERT_RUN_ID"] = self.simulation_id

        self.jobs = []
        for index, job_data in enumerate(job_data_list):
            self.jobs.append(Job(job_data, index))

        self._set_environment()

    def run(self, names_of_jobs_to_run):
        # if names_of_jobs_to_run, create job_queue which contains jobs that
        # are to be run.
        if not names_of_jobs_to_run:
            job_queue = self.jobs
        else:
            job_queue = [j for j in self.jobs if j.name() in names_of_jobs_to_run]

        init_message = Init(
            job_queue,
            self.simulation_id,
            self.ert_pid,
            self.ens_id,
            self.real_id,
            self.step_id,
            self.experiment_id,
        )

        unused = set(names_of_jobs_to_run) - {j.name() for j in job_queue}
        if unused:
            init_message.with_error(
                f"{unused} does not exist. "
                f"Available jobs: {[j.name() for j in self.jobs]}"
            )
            yield init_message
            return
        else:
            yield init_message

        for job in job_queue:
            for status_update in job.run():
                yield status_update

                if not status_update.success():
                    yield Finish().with_error("Not all jobs completed successfully.")
                    return

        yield Finish()

    def _set_environment(self):
        if self.global_environment:
            for key, value in self.global_environment.items():
                for env_key, env_val in os.environ.items():
                    value = value.replace(f"${env_key}", env_val)
                os.environ[key] = value
