"""Entrypoint for the (internal) API hosting API Preprocessor methods."""

import sys

import lib.environment as env
from lib import schemas, utils
from lib.exceptions import QueueEmptyError
from lib.handlers import (
    CocipHandler,
    PubSubPublishHandler,
    PubSubSubscriptionHandler,
)
from lib.log import format_traceback, logger


def run(
    bq_publish_handler: PubSubPublishHandler,
    job_handler: PubSubSubscriptionHandler,
    sigterm_handler: utils.SigtermHandler,
) -> None:
    """
    Generate grids and regions data product,
    and write it to a location backing the /v1 public API.

    This service  consumes jobs form Pubsub.
    Each payload containing a JSON representation of an APIPreprocessorJob.

    The HRES ETL service is responsible for generating and publishing API Preprocessor jobs.
    """
    logger.info("initiating run()")
    for message in job_handler.subscribe():
        if sigterm_handler.should_exit:
            sys.exit(0)

        job = schemas.ApiPreprocessorJob.from_utf8_json(message.data)

        # TODO: move the following into a validation handler (ticketed)
        # prune jobs where hres met data availability isn't sufficient
        # -------
        # The current (0.49.3) pycontrails implementation needs a buffer
        # for differencing accumulated radiative fluxes.
        # 1) job.model_predicted_at must be at least half an hour after job.model_run_at
        # 2) prediction_runway_hrs must extend at least half an hour beyond CocipHandler.MAX_AGE_HR
        prediction_wall = (
            job.model_run_at + 72 * 60 * 60
        )  # 72 hrs of fwd met data per model run
        prediction_runway_hrs = (prediction_wall - job.model_predicted_at) / (60 * 60)
        logger.info(
            f"job should have {prediction_runway_hrs} of forward-looking hres data"
        )
        if (
            job.model_predicted_at < job.model_run_at + 1800  # seconds in 0.5 hr
            or prediction_runway_hrs < CocipHandler.MAX_AGE_HR + 0.5
        ):
            logger.info(f"skipping. not enough met data for job: {job}")
            job_handler.ack(message)
            continue

        logger.info(f"generating outputs for job. job: {job}")
        # ===================
        # run CoCip grid model for the job
        # ===================
        cocip_handler = CocipHandler(
            env.SOURCE_PATH,
            job,
            f"{env.SINK_PATH}/grids",
            f"{env.SINK_PATH}/regions",
        )
        cocip_handler.read()
        cocip_handler.compute()
        cocip_handler.write()

        # ===================
        # publish regions geojson to BQ
        # ===================
        for thres, geo in cocip_handler.regions:
            blob = schemas.RegionsBigQuery(
                aircraft_class=job.aircraft_class,
                flight_level=job.flight_level,
                timestamp=job.model_predicted_at,
                hres_model_run_at=job.model_run_at,
                threshold=thres,
                regions=geo,
            )
            bq_publish_handler.publish_async(
                data=blob.to_bq_flatmap(),
                timeout_seconds=45,
            )
        bq_publish_handler.wait_for_publish(timeout_seconds=60)

        job_handler.ack(message)
        logger.info(f"processing of job complete. job: {job}")


if __name__ == "__main__":
    logger.info("starting api-preprocessor instance")

    try:
        bq_publish_handler = PubSubPublishHandler(
            topic_id=env.COCIP_REGIONS_BQ_TOPIC_ID,
            ordered_queue=False,
        )
        job_handler = PubSubSubscriptionHandler(env.API_PREPROCESSOR_SUBSCRIPTION_ID)
        sigterm_handler = utils.SigtermHandler()
        run(
            bq_publish_handler=bq_publish_handler,
            job_handler=job_handler,
            sigterm_handler=sigterm_handler,
        )

    except QueueEmptyError:
        logger.info("No more messages. Exiting...")
        sys.exit(0)

    except Exception:
        logger.error("Unhandled exception:" + format_traceback())
        sys.exit(1)
