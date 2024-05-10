"""
Scratch scripting to manually inject a single job into the api-preprocessor queue.

Forces reprocessing of a given job, or, processing of a job that leaked/failed to run.

Emulates the behavior of publishing jobs from the hres-etl service.

This may/should be turned into a Makefile or CLI tool.
"""

from lib.handlers import PubSubPublishHandler
from lib.schemas import ApiPreprocessorJob
from datetime import datetime

API_PREPROCESSOR_JOB_PUBSUB_TOPIC_ID = (
    "projects/contrails-301217/topics/dev-mpl-api-parcel-topic"
)

pub_handler = PubSubPublishHandler(
    API_PREPROCESSOR_JOB_PUBSUB_TOPIC_ID,
    ordered_queue=False,
)

model_run_at_dtstr = "2024-05-09T06:00:00Z"
model_predicted_at_dtstr = "2024-05-09T08:00:00Z"

targets = [
    {
        "flight_level": 310,
        "threshold": 500000000,
        "model_run_at": "2024-05-08T00:00:00Z",
        "model_predicted_at": "2024-05-08T00:00:00Z",
    },
    {
        "flight_level": 310,
        "threshold": 500000000,
        "model_run_at": "2024-05-08T00:00:00Z",
        "model_predicted_at": "2024-05-08T01:00:00Z",
    },
    {
        "flight_level": 310,
        "threshold": 500000000,
        "model_run_at": "2024-05-08T00:00:00Z",
        "model_predicted_at": "2024-05-08T02:00:00Z",
    },
    {
        "flight_level": 310,
        "threshold": 500000000,
        "model_run_at": "2024-05-08T00:00:00Z",
        "model_predicted_at": "2024-05-08T03:00:00Z",
    },
    {
        "flight_level": 310,
        "threshold": 500000000,
        "model_run_at": "2024-05-08T00:00:00Z",
        "model_predicted_at": "2024-05-08T04:00:00Z",
    },
    {
        "flight_level": 310,
        "threshold": 500000000,
        "model_run_at": "2024-05-08T00:00:00Z",
        "model_predicted_at": "2024-05-08T05:00:00Z",
    },
    {
        "flight_level": 310,
        "threshold": 500000000,
        "model_run_at": "2024-05-08T06:00:00Z",
        "model_predicted_at": "2024-05-08T06:00:00Z",
    },
    {
        "flight_level": 310,
        "threshold": 500000000,
        "model_run_at": "2024-05-08T06:00:00Z",
        "model_predicted_at": "2024-05-08T07:00:00Z",
    },
    {
        "flight_level": 310,
        "threshold": 500000000,
        "model_run_at": "2024-05-08T06:00:00Z",
        "model_predicted_at": "2024-05-08T08:00:00Z",
    },
    {
        "flight_level": 310,
        "threshold": 500000000,
        "model_run_at": "2024-05-08T06:00:00Z",
        "model_predicted_at": "2024-05-08T09:00:00Z",
    },
    {
        "flight_level": 310,
        "threshold": 500000000,
        "model_run_at": "2024-05-08T06:00:00Z",
        "model_predicted_at": "2024-05-08T10:00:00Z",
    },
    {
        "flight_level": 310,
        "threshold": 500000000,
        "model_run_at": "2024-05-08T06:00:00Z",
        "model_predicted_at": "2024-05-08T11:00:00Z",
    },
    {
        "flight_level": 310,
        "threshold": 500000000,
        "model_run_at": "2024-05-08T12:00:00Z",
        "model_predicted_at": "2024-05-08T12:00:00Z",
    },
    {
        "flight_level": 310,
        "threshold": 500000000,
        "model_run_at": "2024-05-08T12:00:00Z",
        "model_predicted_at": "2024-05-08T13:00:00Z",
    },
    {
        "flight_level": 310,
        "threshold": 500000000,
        "model_run_at": "2024-05-08T12:00:00Z",
        "model_predicted_at": "2024-05-08T14:00:00Z",
    },
    {
        "flight_level": 310,
        "threshold": 500000000,
        "model_run_at": "2024-05-08T12:00:00Z",
        "model_predicted_at": "2024-05-08T15:00:00Z",
    },
    {
        "flight_level": 310,
        "threshold": 500000000,
        "model_run_at": "2024-05-08T12:00:00Z",
        "model_predicted_at": "2024-05-08T16:00:00Z",
    },
    {
        "flight_level": 310,
        "threshold": 500000000,
        "model_run_at": "2024-05-08T12:00:00Z",
        "model_predicted_at": "2024-05-08T17:00:00Z",
    },
    {
        "flight_level": 310,
        "threshold": 500000000,
        "model_run_at": "2024-05-08T18:00:00Z",
        "model_predicted_at": "2024-05-08T18:00:00Z",
    },
]

for target in targets:
    mra = target["model_run_at"]
    mpa = target["model_predicted_at"]
    fl = target["flight_level"]
    job = ApiPreprocessorJob(
        model_run_at=int(datetime.fromisoformat(mra).timestamp()),
        model_predicted_at=int(datetime.fromisoformat(mpa).timestamp()),
        flight_level=fl,
        aircraft_class="default",
    )

    pub_handler.publish_async(
        data=job.as_utf8_json(),
        timeout_seconds=45,
        log_context={"msg": "job from manual run"},
    )
pub_handler.wait_for_publish(timeout_seconds=60)
