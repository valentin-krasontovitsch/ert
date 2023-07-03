import time
import collections
import copy
import pandas as pd
import datetime
import re
import typing
from collections import defaultdict
from typing import Any, Dict, Mapping, Optional, Sequence, Union, cast
import pprint

import pyrsistent
from cloudevents.http import CloudEvent
from dateutil.parser import parse
from pydantic import BaseModel
from pyrsistent import freeze
from pyrsistent.typing import PMap as TPMap

from ert.ensemble_evaluator import identifiers as ids
from ert.ensemble_evaluator import state


def _recursive_update(
    left: TPMap[str, Any],
    right: Union[Mapping[str, Any], TPMap[str, Any]],
    check_key: bool = True,
) -> TPMap[str, Any]:
    for k, v in right.items():
        if check_key and k not in left:
            raise ValueError(f"Illegal field {k}")
        if isinstance(v, collections.abc.Mapping):
            d_val = left.get(k)
            if not d_val:
                left = left.set(k, freeze(v))
            else:
                left = left.set(k, _recursive_update(d_val, v, check_key))
        else:
            left = left.set(k, v)
    return left


_regexp_pattern = r"(?<=/{token}/)[^/]+"


def _match_token(token: str, source: str) -> str:
    f_pattern = _regexp_pattern.format(token=token)
    match = re.search(f_pattern, source)
    return match if match is None else match.group()  # type: ignore


def _get_real_id(source: str) -> str:
    return _match_token("real", source)


def _get_step_id(source: str) -> str:
    return _match_token("step", source)


def _get_job_id(source: str) -> str:
    return _match_token("job", source)


def _get_job_index(source: str) -> str:
    return _match_token("index", source)


class UnsupportedOperationException(ValueError):
    pass


_FM_TYPE_EVENT_TO_STATUS = {
    ids.EVTYPE_FM_STEP_WAITING: state.STEP_STATE_WAITING,
    ids.EVTYPE_FM_STEP_PENDING: state.STEP_STATE_PENDING,
    ids.EVTYPE_FM_STEP_RUNNING: state.STEP_STATE_RUNNING,
    ids.EVTYPE_FM_STEP_FAILURE: state.STEP_STATE_FAILURE,
    ids.EVTYPE_FM_STEP_SUCCESS: state.STEP_STATE_SUCCESS,
    ids.EVTYPE_FM_STEP_UNKNOWN: state.STEP_STATE_UNKNOWN,
    ids.EVTYPE_FM_STEP_TIMEOUT: state.STEP_STATE_FAILURE,
    ids.EVTYPE_FM_JOB_START: state.JOB_STATE_START,
    ids.EVTYPE_FM_JOB_RUNNING: state.JOB_STATE_RUNNING,
    ids.EVTYPE_FM_JOB_SUCCESS: state.JOB_STATE_FINISHED,
    ids.EVTYPE_FM_JOB_FAILURE: state.JOB_STATE_FAILURE,
}

_ENSEMBLE_TYPE_EVENT_TO_STATUS = {
    ids.EVTYPE_ENSEMBLE_STARTED: state.ENSEMBLE_STATE_STARTED,
    ids.EVTYPE_ENSEMBLE_STOPPED: state.ENSEMBLE_STATE_STOPPED,
    ids.EVTYPE_ENSEMBLE_CANCELLED: state.ENSEMBLE_STATE_CANCELLED,
    ids.EVTYPE_ENSEMBLE_FAILED: state.ENSEMBLE_STATE_FAILED,
}

_STEP_STATE_TO_REALIZATION_STATE = {
    state.STEP_STATE_WAITING: state.REALIZATION_STATE_WAITING,
    state.STEP_STATE_PENDING: state.REALIZATION_STATE_PENDING,
    state.STEP_STATE_RUNNING: state.REALIZATION_STATE_RUNNING,
    state.STEP_STATE_UNKNOWN: state.REALIZATION_STATE_UNKNOWN,
    state.STEP_STATE_FAILURE: state.REALIZATION_STATE_FAILED,
}


def convert_iso8601_to_datetime(
    timestamp: Union[datetime.datetime, str]
) -> datetime.datetime:
    if isinstance(timestamp, datetime.datetime):
        return timestamp

    return parse(timestamp)


DICT_SEP = "/"


def _flatten_job_data(job_dict: dict) -> dict:
    if "data" in job_dict.keys() and job_dict["data"]:
        for key, value in job_dict["data"].items():
            job_dict["data" + DICT_SEP + key] = value
        del job_dict["data"]
    return job_dict


def _unflatten_job_data(job_dict: dict) -> dict:
    data = {}
    key_prefix = "data" + DICT_SEP
    for key, value in job_dict.items():
        if key.startswith(key_prefix):
            data[key[len(key_prefix) :]] = value
    unflattened_dict = {
        key: value for key, value in job_dict.items() if not key.startswith(key_prefix)
    }
    unflattened_dict["data"] = data
    return unflattened_dict


def _fix_date_dtypes(some_dict: dict) -> dict:
    for time_key in ["start_time", "end_time"]:
        if time_key in some_dict:
            some_dict[time_key] = some_dict[time_key].to_pydatetime()
    return some_dict


def _filter_nones(some_dict: dict) -> dict:
    return {key: value for key, value in some_dict.items() if value is not None}


class PartialSnapshot:
    def __init__(self, a_ignored_snapshot) -> None:
        # 4 lists will also be fine for this, but the DataFrame
        # has nice functions for making dicts, maybe that pays off.
        # There is a question on how to handle None/NaNs together with the update/merges
        # that are to happen.
        self._realization_states = pd.DataFrame(
            columns=["active", "start_time", "end_time", "status"], index=[]
        )

        self._step_states = pd.DataFrame()

        # the job_states is a multiindex dataframe with realization, step_id and job_id as indices.
        self._job_states = pd.DataFrame(
            columns=[
                "status",
                "index",
                "start_time",
                "end_time",
                "error",
                "name",
                "stderr",
                "stdout",
                "data" + DICT_SEP + "memory",
            ],
            index=pd.MultiIndex(
                levels=[[], [], []],
                codes=[[], [], []],
                names=["real_id", "step_id", "job_id"],
            ),
        )
        # Additionally, a column called "data/something" will be added whenever a data-dictionary is added,
        # containing e.g. memory information for the job. We flatten out that information for speed reasons.

        # self._ensemble_state: str = state.ENSEMBLE_STATE_UNKNOWN
        self._ensemble_state: Optional[str] = None
        self._metadata = {}

    @property
    def status(self) -> str:
        return self._ensemble_state

    def update_status(self, status: str) -> None:
        self._ensemble_state = status

    def update_metadata(self, metadata: Dict[str, Any]) -> None:
        self._metadata.update(metadata)
        # todo: we don't' have the full snapshot in this object any longer, and the merge into
        # that will have to be done later!

        # if self._snapshot is None:
        #    raise UnsupportedOperationException(
        #        f"updating metadata on {self.__class__} without providing a snapshot"
        #        + " is not supported"
        #    )
        # dictionary = pyrsistent.pmap({ids.METADATA: metadata})
        # self._data = _recursive_update(self._data, dictionary, check_key=False)
        # self._snapshot.merge_metadata(metadata)

    def update_realization(
        self,
        real_id: int,
        status: str,
        active: bool,
        start_time: datetime.datetime,
        end_time: datetime.datetime,
        steps_for_a_realization
        # real: "RealizationSnapshot",
    ) -> None:
        # This four ifs could be accomplished in one line with a dict.update() operation
        if status is not None:
            self._realization_states.loc[real_id, "status"] = status
        if active is not None:
            self._realization_states.loc[real_id, "active"] = active
        if start_time is not None:
            self._realization_states.loc[real_id, "start_time"] = start_time
        if end_time is not None:
            self._realization_states.loc[real_id, "end_time"] = end_time

        if steps_for_a_realization is not None:
            STEP_ID = 0  # This is always 0 outside "ert3"
            self.update_step(real_id, STEP_ID, steps_for_a_realization)

        # self._apply_update(SnapshotDict(reals={real_id: real}))

    # def _apply_update(self, update: "SnapshotDict") -> None:
    #    if self._snapshot is None:
    #        raise UnsupportedOperationException(
    #            f"trying to mutate {self.__class__} without providing a snapshot is "
    #            + "not supported"
    #        )
    #    dictionary = update.dict(
    #        # These are pydantic features:
    #        exclude_unset=True, exclude_none=True, exclude_defaults=True
    #    )
    #    self._data = _recursive_update(self._data, dictionary, check_key=False)
    #    self._snapshot.merge(dictionary)

    def update_step(self, real_id: int, step_id: int, step) -> "PartialSnapshot":
        print(f"ignoring step={step}")
        # todo: see if we need to adhere to the transfer of step state onto realization state
        return self
        # Skipping the step for now

        # self._apply_update(
        #     SnapshotDict(reals={real_id: RealizationSnapshot(steps={step_id: step})})
        # )
        # if self._snapshot.get_real(real_id).status != state.REALIZATION_STATE_FAILED:
        #     if step.status in _STEP_STATE_TO_REALIZATION_STATE:
        #         self.update_real(
        #             real_id,
        #             RealizationSnapshot(
        #                 status=_STEP_STATE_TO_REALIZATION_STATE[step.status]
        #             ),
        #         )
        #     elif (
        #         step.status == state.REALIZATION_STATE_FINISHED
        #         and self._snapshot.all_steps_finished(real_id)
        #     ):
        #         self.update_real(
        #             real_id,
        #             RealizationSnapshot(status=state.REALIZATION_STATE_FINISHED),
        #         )
        #     elif (
        #         step.status == state.STEP_STATE_SUCCESS
        #         and not self._snapshot.all_steps_finished(real_id)
        #     ):
        #         pass
        #     else:
        #         raise ValueError(
        #             f"unknown step status {step.status} for real: {real_id} step: "
        #             + f"{step_id}"
        #         )
        # return self

    def update_job(
        self,
        real_id: str,
        step_id: str,
        job_id: str,
        job: "Job",
    ) -> "PartialSnapshot":
        job_idx = (real_id, step_id, job_id)
        if job_idx in self._job_states.index:
            job_as_dict = self._job_states.loc[job_idx].to_dict()
            job_as_dict.update(_flatten_job_data(job.dict()))
            self._job_states.loc[job_idx] = job_as_dict
        else:
            self._job_states.loc[job_idx] = _flatten_job_data(job.dict())

        return self
        # self._apply_update(
        #    SnapshotDict(
        #        reals={
        #            real_id: RealizationSnapshot(
        #                steps={step_id: Step(jobs={job_id: job})}
        #            )
        #        }
        #    )
        # )
        # return self

    def to_dict(self) -> Mapping[str, Any]:
        _dict = {}
        if self._metadata:
            _dict["metadata"] = self._metadata
        if self._ensemble_state:
            _dict["status"] = self._ensemble_state
        if not self._realization_states.empty:
            _dict["reals"] = _filter_nones(
                _fix_date_dtypes(self._realization_states.to_dict(orient="index"))
            )

        for job_idx, job_values in self._job_states.iterrows():
            real_id = job_idx[0]
            step_id = job_idx[1]
            job_id = job_idx[2]
            if "reals" not in _dict:
                _dict["reals"] = {real_id: {}}
            _dict["reals"][real_id]["steps"] = {step_id: {"jobs": {}}}
            _dict["reals"][real_id]["steps"][step_id]["jobs"][job_id] = _filter_nones(
                _fix_date_dtypes(_unflatten_job_data(job_values.to_dict()))
            )

        return _dict

    def data(self) -> Mapping[str, Any]:
        return self.to_dict()

    # pylint: disable=too-many-branches
    def from_cloudevent(self, event: CloudEvent) -> "PartialSnapshot":
        start = time.time()
        e_type = event["type"]
        e_source = event["source"]
        status = _FM_TYPE_EVENT_TO_STATUS.get(e_type)
        timestamp = event["time"]

        if self._snapshot is None:
            raise UnsupportedOperationException(
                f"updating {self.__class__} without a snapshot is not supported"
            )

        if e_type in ids.EVGROUP_FM_STEP:
            start_time = None
            end_time = None
            if e_type == ids.EVTYPE_FM_STEP_RUNNING:
                start_time = convert_iso8601_to_datetime(timestamp)
            elif e_type in {
                ids.EVTYPE_FM_STEP_SUCCESS,
                ids.EVTYPE_FM_STEP_FAILURE,
                ids.EVTYPE_FM_STEP_TIMEOUT,
            }:
                end_time = convert_iso8601_to_datetime(timestamp)

            self.update_step(
                _get_real_id(e_source),
                _get_step_id(e_source),
                step=Step(
                    status=status,
                    start_time=start_time,
                    end_time=end_time,
                ),
            )

            if e_type == ids.EVTYPE_FM_STEP_TIMEOUT:
                step = self._snapshot.get_step(
                    _get_real_id(e_source), _get_step_id(e_source)
                )
                for job_id, job in step.jobs.items():
                    if job.status != state.JOB_STATE_FINISHED:
                        job_error = "The run is cancelled due to reaching MAX_RUNTIME"
                        job_index = _get_job_index(e_source)
                        self.update_job(
                            _get_real_id(e_source),
                            _get_step_id(e_source),
                            job_id,
                            job=Job(
                                status=state.JOB_STATE_FAILURE,
                                index=job_index,
                                error=job_error,
                            ),
                        )

        elif e_type in ids.EVGROUP_FM_JOB:
            start_time = None
            end_time = None
            if e_type == ids.EVTYPE_FM_JOB_START:
                start_time = convert_iso8601_to_datetime(timestamp)
            elif e_type in {ids.EVTYPE_FM_JOB_SUCCESS, ids.EVTYPE_FM_JOB_FAILURE}:
                end_time = convert_iso8601_to_datetime(timestamp)
            job_index = _get_job_index(e_source)
            self.update_job(
                _get_real_id(e_source),
                _get_step_id(e_source),
                _get_job_id(e_source),
                job=Job(
                    status=status,
                    start_time=start_time,
                    end_time=end_time,
                    index=job_index,
                    data=event.data if e_type == ids.EVTYPE_FM_JOB_RUNNING else None,
                    stdout=event.data.get(ids.STDOUT)
                    if e_type == ids.EVTYPE_FM_JOB_START
                    else None,
                    stderr=event.data.get(ids.STDERR)
                    if e_type == ids.EVTYPE_FM_JOB_START
                    else None,
                    error=event.data.get(ids.ERROR_MSG)
                    if e_type == ids.EVTYPE_FM_JOB_FAILURE
                    else None,
                ),
            )
        elif e_type in ids.EVGROUP_ENSEMBLE:
            self.update_status(_ENSEMBLE_TYPE_EVENT_TO_STATUS[e_type])
        elif e_type == ids.EVTYPE_EE_SNAPSHOT_UPDATE:
            self._data = _recursive_update(self._data, event.data, check_key=False)
        else:
            raise ValueError(f"Unknown type: {e_type}")
        print("from-cl took: ")
        print(time.time() - start)
        return self


class Snapshot:
    def __init__(self, input_dict: Mapping[str, Any]) -> None:
        self._data: TPMap[str, Any] = pyrsistent.freeze(input_dict)

    def merge_event(self, event: PartialSnapshot) -> None:
        print("MERGE EVENTV")
        pprint.pprint(event.to_dict())
        pprint.pprint(self.to_dict())
        self._data = _recursive_update(self._data, event.data())
        print(" **' 'after*** ")
        pprint.pprint(self.to_dict())

    def merge(self, update: Mapping[str, Any]) -> None:
        print("MERGE *****'")
        pprint.pprint(self.to_dict())
        pprint.pprint(update)
        self._data = _recursive_update(self._data, update)
        print(" **' 'after*** ")
        pprint.pprint(self.to_dict())

    def merge_metadata(self, metadata: Dict[str, Any]) -> None:
        print("MERGE metadata *****'")
        pprint.pprint(self.to_dict())
        pprint.pprint(metadata)
        self._data = _recursive_update(
            self._data, pyrsistent.pmap({ids.METADATA: metadata}), check_key=False
        )
        print(" **' 'after*** ")
        pprint.pprint(self.to_dict())

    def to_dict(self) -> Mapping[str, Any]:
        return cast(Mapping[str, Any], pyrsistent.thaw(self._data))

    @property
    def status(self) -> str:
        return cast(str, self._data[ids.STATUS])

    @property
    def reals(self) -> Dict[str, "RealizationSnapshot"]:
        return SnapshotDict(**self._data).reals

    def get_real(self, real_id: str) -> "RealizationSnapshot":
        if real_id not in self._data[ids.REALS]:
            raise ValueError(f"No realization with id {real_id}")
        return RealizationSnapshot(**self._data[ids.REALS][real_id])

    def get_step(self, real_id: str, step_id: str) -> "Step":
        real = self.get_real(real_id)
        steps = real.steps
        if step_id not in steps:
            raise ValueError(f"No step with id {step_id} in {real_id}")
        return steps[step_id]

    def get_job(self, real_id: str, step_id: str, job_id: str) -> "Job":
        step = self.get_step(real_id, step_id)
        jobs = step.jobs
        if job_id not in jobs:
            raise ValueError(f"No job with id {job_id} in {step_id}")
        return jobs[job_id]

    def all_steps_finished(self, real_id: str) -> bool:
        real = self.get_real(real_id)
        return all(
            step.status == state.STEP_STATE_SUCCESS for step in real.steps.values()
        )

    def get_successful_realizations(self) -> int:
        return len(
            [
                real
                for real in self._data[ids.REALS].values()
                if real[ids.STATUS] == state.REALIZATION_STATE_FINISHED
            ]
        )

    def aggregate_real_states(self) -> typing.Dict[str, int]:
        states: Dict[str, int] = defaultdict(int)
        for real in self._data[ids.REALS].values():
            states[real[ids.STATUS]] += 1
        return states

    def data(self) -> Mapping[str, Any]:
        return self._data


class Job(BaseModel):
    status: Optional[str]
    start_time: Optional[datetime.datetime]
    end_time: Optional[datetime.datetime]
    index: Optional[str]
    data: Optional[Dict[str, Any]]
    name: Optional[str]
    error: Optional[str]
    stdout: Optional[str]
    stderr: Optional[str]


class Step(BaseModel):
    status: Optional[str]
    start_time: Optional[datetime.datetime]
    end_time: Optional[datetime.datetime]
    jobs: Dict[str, Job] = {}


class RealizationSnapshot(BaseModel):
    status: Optional[str]
    active: Optional[bool]
    start_time: Optional[datetime.datetime]
    end_time: Optional[datetime.datetime]
    steps: Dict[str, Step] = {}


class SnapshotDict(BaseModel):
    status: Optional[str] = state.ENSEMBLE_STATE_UNKNOWN
    reals: Dict[str, RealizationSnapshot] = {}
    metadata: Dict[str, Any] = {}


class SnapshotBuilder(BaseModel):
    steps: Dict[str, Step] = {}
    metadata: Dict[str, Any] = {}

    def build(
        self,
        real_ids: Sequence[str],
        status: Optional[str],
        start_time: Optional[datetime.datetime] = None,
        end_time: Optional[datetime.datetime] = None,
    ) -> Snapshot:
        top = SnapshotDict(status=status, metadata=self.metadata)
        for r_id in real_ids:
            top.reals[r_id] = RealizationSnapshot(
                active=True,
                steps=self.steps,
                start_time=start_time,
                end_time=end_time,
                status=status,
            )
        return Snapshot(top.dict())

    def add_step(
        self,
        step_id: str,
        status: Optional[str],
        start_time: Optional[datetime.datetime] = None,
        end_time: Optional[datetime.datetime] = None,
    ) -> "SnapshotBuilder":
        self.steps[step_id] = Step(
            status=status, start_time=start_time, end_time=end_time
        )
        return self

    def add_job(  # pylint: disable=too-many-arguments
        self,
        step_id: str,
        # This is not tested in test_snapshot.py..
        job_id: str,
        index: str,
        name: Optional[str],
        status: Optional[str],
        data: Optional[Dict[str, Any]],
        start_time: Optional[datetime.datetime] = None,
        end_time: Optional[datetime.datetime] = None,
        stdout: Optional[str] = None,
        stderr: Optional[str] = None,
    ) -> "SnapshotBuilder":
        step = self.steps[step_id]
        step.jobs[job_id] = Job(
            status=status,
            index=index,
            data=data,
            start_time=start_time,
            end_time=end_time,
            name=name,
            stdout=stdout,
            stderr=stderr,
        )
        return self

    def add_metadata(self, key: str, value: Any) -> "SnapshotBuilder":
        self.metadata[key] = value
        return self
