"""Entrypoint for the (internal) API hosting API Preprocessor methods."""

import sys

import lib.environment as env
from lib import schemas, utils
from lib.exceptions import QueueEmptyError
from lib.handlers import (
    CocipHandler,
    PubSubPublishHandler,
    PubSubSubscriptionHandler,
    ValidationHandler,
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

        # TODO: remove bridge
        # temp code to bridge deployment to new logic
        if (job.flight_level != 270) and (
            job.flight_level != job.ALL_FLIGHT_LEVELS_WILDCARD
        ):
            logger.info(f"got fl {job.flight_level}. skipping.")
            job_handler.ack(message)
            continue

        # ===================
        # validate work
        # ===================
        validation_handler = ValidationHandler(job)
        if not validation_handler.sufficient_forecast_range():
            logger.info("insufficient forecast range. skipping job...")
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
        try:
            cocip_handler.compute()
        except Exception:
            logger.error(
                f"failed to compute cocip grid. skipping... job: {job}. "
                f"traceback: {format_traceback()}"
            )
            job_handler.ack(message)
            continue
        cocip_handler.write()

        # ===================
        # publish regions geojson to BQ
        # ===================
        for fl, thres_lookup in cocip_handler.regions.items():
            for thres, geo in thres_lookup.items():
                blob = schemas.RegionsBigQuery(
                    aircraft_class=job.aircraft_class,
                    flight_level=fl,
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
