"""
Scratch scripting to manually inject a single job into the api-preprocessor queue.

Forces reprocessing of a given job, or, processing of a job that leaked/failed to run.

Emulates the behavior of publishing jobs from the hres-etl service.

This may/should be turned into a Makefile or CLI tool.
"""

from lib.handlers import PubSubPublishHandler
from lib.schemas import ApiPreprocessorJob

API_PREPROCESSOR_JOB_PUBSUB_TOPIC_ID = (
    "projects/contrails-301217/topics/dev-mpl-api-parcel-topic"
)

pub_handler = PubSubPublishHandler(
    API_PREPROCESSOR_JOB_PUBSUB_TOPIC_ID,
    ordered_queue=False,
)

model_run_at = 1726466400
flight_level = -1
aircraft_class = "default"

model_predicted_at = [
    1726477200,
    1726470000,
    1726639200,
    1726473600,
    1726502400,
    1726480800,
    1726484400,
    1726498800,
    1726495200,
    1726488000,
    1726563600,
    1726506000,
    1726531200,
    1726527600,
    1726524000,
    1726534800,
    1726538400,
    1726513200,
    1726567200,
    1726560000,
    1726574400,
    1726585200,
    1726581600,
    1726509600,
    1726491600,
    1726556400,
    1726520400,
    1726599600,
    1726610400,
    1726552800,
    1726516800,
    1726542000,
    1726617600,
    1726545600,
    1726549200,
    1726621200,
    1726624800,
    1726614000,
    1726606800,
    1726653600,
    1726646400,
    1726657200,
    1726650000,
    1726664400,
    1726570800,
    1726578000,
    1726588800,
    1726592400,
    1726596000,
    1726603200,
    1726671600,
    1726668000,
    1726660800,
    1726675200,
    1726678800,
    1726628400,
    1726642800,
    1726632000,
    1726635600,
]

for target in model_predicted_at:
    job = ApiPreprocessorJob(
        model_run_at=model_run_at,
        model_predicted_at=target,
        flight_level=flight_level,
        aircraft_class=aircraft_class,
    )
    print(job.as_utf8_json())

    pub_handler.publish_async(
        data=job.as_utf8_json(),
        timeout_seconds=45,
        log_context={"msg": "job from manual run"},
    )
pub_handler.wait_for_publish(timeout_seconds=60)
