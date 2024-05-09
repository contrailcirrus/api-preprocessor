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
    "projects/contrails-301217/topics/prod-mpl-api-parcel-topic"
)

pub_handler = PubSubPublishHandler(
    API_PREPROCESSOR_JOB_PUBSUB_TOPIC_ID,
    ordered_queue=False,
)

model_run_at_dtstr = "2024-05-09T06:00:00Z"
model_predicted_at_dtstr = "2024-05-09T08:00:00Z"

job = ApiPreprocessorJob(
    model_run_at=int(datetime.fromisoformat(model_run_at_dtstr).timestamp()),
    model_predicted_at=int(
        datetime.fromisoformat(model_predicted_at_dtstr).timestamp()
    ),
    flight_level=310,
    aircraft_class="default",
)

pub_handler.publish_async(
    data=job.as_utf8_json(),
    timeout_seconds=45,
    log_context={"msg": "job from manual run"},
)
pub_handler.wait_for_publish(timeout_seconds=60)
