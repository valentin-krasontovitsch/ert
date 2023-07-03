import uuid
from typing import Dict

import pytest
from cloudevents.http.event import CloudEvent

from ert.ensemble_evaluator import identifiers as ids
from ert.ensemble_evaluator import state
from ert.ensemble_evaluator.snapshot import (
    Job,
    PartialSnapshot,
    RealizationSnapshot,
    Snapshot,
    SnapshotDict,
    Step,
)


@pytest.mark.parametrize(
    "ensemble_size, forward_models, memory_reports",
    [
        (10, 10, 1),
        (100, 10, 1),
        (10, 100, 1),
        (10, 10, 10),
    ],
)
def test_snapshot_handling_of_forward_model_events(
    benchmark, ensemble_size, forward_models, memory_reports
):
    benchmark(
        simulate_forward_model_event_handling,
        ensemble_size,
        forward_models,
        memory_reports,
    )


def simulate_forward_model_event_handling(
    ensemble_size, forward_models, memory_reports
):
    reals: Dict[str, RealizationSnapshot] = {}
    for real in range(ensemble_size):
        reals[str(real)] = RealizationSnapshot(
            active=True,
            status=state.REALIZATION_STATE_WAITING,
        )
        reals[str(real)].steps["0"] = Step(status=state.STEP_STATE_UNKNOWN)
        for job_idx in range(forward_models):
            reals[f"{real}"].steps["0"].jobs[str(job_idx)] = Job(
                status=state.JOB_STATE_START,
                index=job_idx,
                data={},
                name=f"FM_{job_idx}",
            )
    top = SnapshotDict(
        reals=reals, status=state.ENSEMBLE_STATE_UNKNOWN, metadata={"foo": "bar"}
    )

    snapshot = Snapshot(top.dict())

    partial = PartialSnapshot(snapshot)

    ens_id = "A"
    partial.from_cloudevent(
        CloudEvent(
            {
                "source": f"/ert/ensemble/{ens_id}",
                "type": ids.EVTYPE_ENSEMBLE_STARTED,
                "id": str(uuid.uuid1()),
            }
        )
    )

    # Send STEP_WAITING for every realization:
    for real in range(ensemble_size):
        partial.from_cloudevent(
            CloudEvent(
                {
                    "source": f"/ert/ensemble/{ens_id}/real/{real}/step/0",
                    "type": ids.EVTYPE_FM_STEP_WAITING,
                    "id": str(uuid.uuid1()),
                }
            )
        )

    # Send forward model start for each realization and job
    for job_idx in range(forward_models):
        for real in range(ensemble_size):
            partial.from_cloudevent(
                CloudEvent(
                    attributes={
                        "source": f"/ert/ensemble/{ens_id}/"
                        f"real/{real}/step/0/job/{job_idx}",
                        "type": ids.EVTYPE_FM_JOB_START,
                        "id": str(uuid.uuid1()),
                    },
                    data={"stderr": "foo", "stdout": "bar"},
                )
            )
        for current_memory_usage in range(memory_reports):
            for real in range(ensemble_size):
                partial.from_cloudevent(
                    CloudEvent(
                        attributes={
                            "source": f"/ert/ensemble/{ens_id}/real/{real}/step/0/job/{job_idx}",
                            "type": ids.EVTYPE_FM_JOB_RUNNING,
                            "id": str(uuid.uuid1()),
                        },
                        data={
                            "max_memory_usage": current_memory_usage,
                            "current_memory_usage": current_memory_usage,
                        },
                    )
                )
        for real in range(ensemble_size):
            partial.from_cloudevent(
                CloudEvent(
                    attributes={
                        "source": f"/ert/ensemble/{ens_id}/real/{real}/step/0/job/{job_idx}",
                        "type": ids.EVTYPE_FM_JOB_SUCCESS,
                        "id": str(uuid.uuid1()),
                    },
                )
            )

    for real in range(ensemble_size):
        partial.from_cloudevent(
            CloudEvent(
                {
                    "source": f"/ert/ensemble/{ens_id}/real/{real}/step/0",
                    "type": ids.EVTYPE_FM_STEP_SUCCESS,
                    "id": str(uuid.uuid1()),
                }
            )
        )

    # What we must send:
    # com.equinor.ert.ensemble.started

    # com.equinor.ert.forward_model_step.waiting, for each step and realization
    # com.equinor.ert.forward_model_job.start, for each real, step and job, stdin++ in event-data-dict
    # com.equinor.ert.forward_model_job.running, ^. With max_memory_usage and current_memory_usage,
    # repeat above 100 times
    # com.equinor.ert.forward_model_job.success for real, step, job
    # com.equinor.ert.forward_model_step.running  for real, step.
    # com.equinor.ert.ee.snapshot_update, data=status:Starting (the above is not merged in it seems)

    # com.equinor.ert.ee.snapshot_update, data er er hele sulamitten. FM har status Finished, men ikke step/real.
    # com.equinor.ert.forward_model_step.success, for real, step
    # com.equinor.ert.ensemble.stopped data=None

    # com.equinor.ert.ee.snapshot_update
    # com.equinor.ert.ee.snapshot_update
    # hver av disse inkluderer to merge_event()-kall., totalt 12 stk. 12 stk også ved 1 FM

    # merge() kalles totalt 24 ganger, 13 hvis bare en FM, ser ut som det er dobbelt for noen fm.
    # merge er tilsynelatende dobbel fordi det gjøres både på step og på realisasjonsnivå

    # snapshot = SnapshotBuilder().add_step(step_id="0", status="Unknown")
    # for forward_model_idx in range(FORWARD_MODEL_COUNT):
    #    snapshot.add_job(
    #        "0",
    #        forward_model_idx,
    #        f"{forward_model_idx}",
    #        f"FM_{forward_model_idx}",
    #        state.JOB_STATE_START,
    #        data={},
    #    )
    # snapshot.build([str(real_id) for real_id in range(ENSEMBLE_SIZE)], status="Unknown")
    # snapshot.add_metadata("foo", "bar")


# Poly case with two realizations and a `print(event)` in `snapshot.from_cloudevent()`

# {'attributes': {'specversion': '1.0', 'id':
# '3143c64c-197e-11ee-8519-5065f321aa76', 'source': '/ert/ensemble/310097b4',
# 'type': 'com.equinor.ert.ensemble.started', 'time':
# '2023-07-03T08:47:07.816334+00:00'}, 'data': None}

# {'attributes': {'specversion': '1.0', 'id':
# '12cb5c43-af0d-490d-b91c-0566c94fbdee', 'source':
# '/ert/ensemble/310097b4/real/0/step/0', 'type':
# 'com.equinor.ert.forward_model_step.waiting', 'datacontenttype':
# 'application/json', 'time': '2023-07-03T08:47:07.929162+00:00'}, 'data':
# {'queue_event_type': 'JOB_QUEUE_WAITING'}}

# {'attributes': {'specversion': '1.0', 'id':
# '301d1588-494d-4f3e-94a8-6b74225e28f7', 'source':
# '/ert/ensemble/310097b4/real/1/step/0', 'type':
# 'com.equinor.ert.forward_model_step.waiting', 'datacontenttype':
# 'application/json', 'time': '2023-07-03T08:47:07.929221+00:00'}, 'data':
# {'queue_event_type': 'JOB_QUEUE_WAITING'}}

# {'attributes': {'specversion': '1.0', 'id':
# 'aa0bd46d-cca1-4eac-b0cb-a2389ed5558e', 'source':
# '/ert/ensemble/310097b4/real/0/step/0/job/0/index/0', 'type':
# 'com.equinor.ert.forward_model_job.start', 'datacontenttype':
# 'application/json', 'time': '2023-07-03T08:47:08.218413+00:00'}, 'data':
# {'stdout':
# '/work/projects/ert/test-data/poly_example/poly_out/realization-0/iter-0/poly_eval.stdout.0',
# 'stderr':
# '/work/projects/ert/test-data/poly_example/poly_out/realization-0/iter-0/poly_eval.stderr.0'}}

# {'attributes': {'specversion': '1.0', 'id':
# 'c5f2dd7a-831b-412f-a018-3c9e765bd9ed', 'source':
# '/ert/ensemble/310097b4/real/0/step/0/job/0/index/0', 'type':
# 'com.equinor.ert.forward_model_job.running', 'datacontenttype':
# 'application/json', 'time': '2023-07-03T08:47:08.225110+00:00'}, 'data':
# {'max_memory_usage': 1708032, 'current_memory_usage': 1708032}}

# {'attributes': {'specversion': '1.0', 'id':
# 'f8040fa2-a1f2-4cfb-a4f8-5fab346d57f5', 'source':
# '/ert/ensemble/310097b4/real/1/step/0/job/0/index/0', 'type':
# 'com.equinor.ert.forward_model_job.start', 'datacontenttype':
# 'application/json', 'time': '2023-07-03T08:47:08.249948+00:00'}, 'data':
# {'stdout':
# '/work/projects/ert/test-data/poly_example/poly_out/realization-1/iter-0/poly_eval.stdout.0',
# 'stderr':
# '/work/projects/ert/test-data/poly_example/poly_out/realization-1/iter-0/poly_eval.stderr.0'}}

# {'attributes': {'specversion': '1.0', 'id':
# '3ddea1e1-8b46-496d-8f28-e23a55d92835', 'source':
# '/ert/ensemble/310097b4/real/1/step/0/job/0/index/0', 'type':
# 'com.equinor.ert.forward_model_job.running', 'datacontenttype':
# 'application/json', 'time': '2023-07-03T08:47:08.255678+00:00'}, 'data':
# {'max_memory_usage': 368640, 'current_memory_usage': 368640}}

# {'attributes': {'specversion': '1.0', 'id':
# 'ac861ec3-7042-4408-9b22-beeed9e0f0de', 'source':
# '/ert/ensemble/310097b4/real/1/step/0/job/0/index/0', 'type':
# 'com.equinor.ert.forward_model_job.success', 'time':
# '2023-07-03T08:47:08.308694+00:00'}, 'data': None}

# {'attributes': {'specversion': '1.0', 'id':
# '981c0d99-a887-41fe-8fa9-c261de8130f2', 'source':
# '/ert/ensemble/310097b4/real/0/step/0/job/0/index/0', 'type':
# 'com.equinor.ert.forward_model_job.success', 'time':
# '2023-07-03T08:47:08.318184+00:00'}, 'data': None}

# {'attributes': {'specversion': '1.0', 'id':
# '30f7c0a2-a29d-4eec-b180-b60148f2d2ec', 'source':
# '/ert/ensemble/310097b4/real/0/step/0', 'type':
# 'com.equinor.ert.forward_model_step.running', 'datacontenttype':
# 'application/json', 'time': '2023-07-03T08:47:09.013971+00:00'}, 'data':
# {'queue_event_type': 'JOB_QUEUE_RUNNING'}}

# {'attributes': {'specversion': '1.0', 'id':
# '024e548d-e180-4f47-bbd3-d29835fa8cf7', 'source':
# '/ert/ensemble/310097b4/real/1/step/0', 'type':
# 'com.equinor.ert.forward_model_step.running', 'datacontenttype':
# 'application/json', 'time': '2023-07-03T08:47:09.014065+00:00'}, 'data':
# {'queue_event_type': 'JOB_QUEUE_RUNNING'}}

# {'attributes': {'specversion': '1.0', 'id':
# 'f6782669-2133-401e-8e1a-45e22eea1f7b', 'source': '/ert/ensemble/310097b4',
# 'type': 'com.equinor.ert.ee.snapshot_update', 'time':
# '2023-07-03T08:47:09.383086+00:00'}, 'data': {'status': 'Starting', 'iter':
# 0}}

# {'attributes': {'specversion': '1.0', 'id':
# '1d15617f-c8ac-456f-9114-dbfaa290fef9', 'source': '/ert/ensemble/310097b4',
# 'type': 'com.equinor.ert.ee.snapshot_update', 'time':
# '2023-07-03T08:47:09.403190+00:00'}, 'data': {'reals': {'1': {'status':
# 'Running', 'steps': {'0': {'start_time': datetime.datetime(2023, 7, 3, 8, 47,
# 9, 14065, tzinfo=tzutc()), 'jobs': {'0': {'data': {'current_memory_usage':
# 368640, 'max_memory_usage': 368640}, 'status': 'Finished', 'start_time':
# datetime.datetime(2023, 7, 3, 8, 47, 8, 249948, tzinfo=tzutc()), 'stdout':
# '/work/projects/ert/test-data/poly_example/poly_out/realization-1/iter-0/poly_eval.stdout.0',
# 'stderr':
# '/work/projects/ert/test-data/poly_example/poly_out/realization-1/iter-0/poly_eval.stderr.0',
# 'end_time': datetime.datetime(2023, 7, 3, 8, 47, 8, 308694, tzinfo=tzutc()),
# 'index': '0'}}, 'status': 'Running'}}}, '0': {'status': 'Running', 'steps':
# {'0': {'start_time': datetime.datetime(2023, 7, 3, 8, 47, 9, 13971,
# tzinfo=tzutc()), 'jobs': {'0': {'data': {'current_memory_usage': 1708032,
# 'max_memory_usage': 1708032}, 'status': 'Finished', 'start_time':
# datetime.datetime(2023, 7, 3, 8, 47, 8, 218413, tzinfo=tzutc()), 'stdout':
# '/work/projects/ert/test-data/poly_example/poly_out/realization-0/iter-0/poly_eval.stdout.0',
# 'stderr':
# '/work/projects/ert/test-data/poly_example/poly_out/realization-0/iter-0/poly_eval.stderr.0',
# 'end_time': datetime.datetime(2023, 7, 3, 8, 47, 8, 318184, tzinfo=tzutc()),
# 'index': '0'}}, 'status': 'Running'}}}}, 'iter': 0}}

# {'attributes': {'specversion': '1.0', 'id':
# '93fa8f0d-86a7-4ff8-9cea-c738c62bec45', 'source':
# '/ert/ensemble/310097b4/real/0/step/0', 'type':
# 'com.equinor.ert.forward_model_step.success', 'datacontenttype':
# 'application/json', 'time': '2023-07-03T08:47:10.017807+00:00'}, 'data':
# {'queue_event_type': 'JOB_QUEUE_SUCCESS'}}

# {'attributes': {'specversion': '1.0', 'id':
# '6ddeba06-7fb1-47aa-9067-0b69f209456d', 'source':
# '/ert/ensemble/310097b4/real/1/step/0', 'type':
# 'com.equinor.ert.forward_model_step.success', 'datacontenttype':
# 'application/json', 'time': '2023-07-03T08:47:10.017927+00:00'}, 'data':
# {'queue_event_type': 'JOB_QUEUE_SUCCESS'}}

# {'attributes': {'specversion': '1.0', 'id':
# '388b19ce-008c-4840-bb74-28fa2e3c4eee', 'source':
# '/ert/ensemble/310097b4/real/0/step/0', 'type':
# 'com.equinor.ert.forward_model_step.success', 'datacontenttype':
# 'application/json', 'time': '2023-07-03T08:47:10.048958+00:00'}, 'data':
# {'queue_event_type': 'JOB_QUEUE_SUCCESS'}}

# {'attributes': {'specversion': '1.0', 'id':
# '1e0cfd5f-503e-4a83-b18f-e305a1ea5cd8', 'source':
# '/ert/ensemble/310097b4/real/1/step/0', 'type':
# 'com.equinor.ert.forward_model_step.success', 'datacontenttype':
# 'application/json', 'time': '2023-07-03T08:47:10.049017+00:00'}, 'data':
# {'queue_event_type': 'JOB_QUEUE_SUCCESS'}}

# {'attributes': {'specversion': '1.0', 'id':
# '3143c1f6-197e-11ee-8519-5065f321aa76', 'source': '/ert/ensemble/310097b4',
# 'type': 'com.equinor.ert.ensemble.stopped', 'time':
# '2023-07-03T08:47:07.816244+00:00'}, 'data': None}

# {'attributes': {'specversion': '1.0', 'id':
# '72cdedc7-7048-4a55-9db2-fc158bd76122', 'source': '/ert/ensemble/310097b4',
# 'type': 'com.equinor.ert.ee.snapshot_update', 'time':
# '2023-07-03T08:47:11.424224+00:00'}, 'data': {'reals': {'1': {'status':
# 'Finished', 'steps': {'0': {'end_time': datetime.datetime(2023, 7, 3, 8, 47,
# 10, 49017, tzinfo=tzutc()), 'status': 'Finished'}}}, '0': {'status':
# 'Finished', 'steps': {'0': {'end_time': datetime.datetime(2023, 7, 3, 8, 47,
# 10, 48958, tzinfo=tzutc()), 'status': 'Finished'}}}}, 'iter': 0}}

# {'attributes': {'specversion': '1.0', 'id':
# 'fc7e4f98-1390-47dc-adc3-7c14acd6b0a7', 'source': '/ert/ensemble/310097b4',
# 'type': 'com.equinor.ert.ee.snapshot_update', 'time':
# '2023-07-03T08:47:11.424738+00:00'}, 'data': {'status': 'Stopped', 'iter': 0}}
