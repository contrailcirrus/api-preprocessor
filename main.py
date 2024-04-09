"""Entrypoint for the (internal) API hosting API Preprocessor methods."""

import sys

from lib.handlers import (
    CocipHandler,
    JobSubscriptionHandler,
    ValidationHandler,
    PubSubPublishHandler,
)
from lib.schemas import RegionsBigQuery
import lib.environment as env
from lib import utils
from lib.exceptions import QueueEmptyError
from lib.log import format_traceback, logger


def run():
    """
    Generate grids and regions data product,
    and write it to a location backing the /v1 public API.

    This service  consumes jobs form Pubsub.
    Each payload containing a JSON representation of an APIPreprocessorJob.

    The HRES ETL service is responsible for generating and publishing API Preprocessor jobs.
    """
    logger.info("initiating run()")
    with JobSubscriptionHandler(env.API_PREPROCESSOR_SUBSCRIPTION_ID) as job_handler:
        job = job_handler.fetch()

        # ===================
        # validate work
        # ===================
        validation_handler = ValidationHandler(job)
        if not validation_handler.sufficient_forecast_range():
            logger.info("insufficient forecast range. skipping job...")
            job_handler.ack()
            return

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
        bq_publish_handler = PubSubPublishHandler("bq_topic")
        for thres, geo in cocip_handler.regions:
            blob = RegionsBigQuery(
                aircraft_class=job.aircraft_class,
                flight_level=job.flight_level,
                timestamp=job.model_predicted_at,
                hres_model_run_at=job.model_run_at,
                threshold=thres,
                regions=geo,
            )
            bq_publish_handler.publish_async(data=blob.to_bq_flatmap())
        bq_publish_handler.wait_for_publish()

        job_handler.ack()
    logger.info(f"processing of job complete. job: {job}")


if __name__ == "__main__":
    logger.info("starting api-preprocessor instance")
    sigterm_handler = utils.SigtermHandler()
    while True:
        if sigterm_handler.should_exit:
            sys.exit(0)
        try:
            run()
        except QueueEmptyError:
            logger.info("No more messages. Exiting...")
            sys.exit(0)
        except Exception:
            logger.error("Unhandled exception:" + format_traceback())
            sys.exit(1)
