"""Entrypoint for the (internal) API hosting API Preprocessor methods."""

import sys

from lib.handlers import CocipHandler, JobSubscriptionHandler, ZarrRemoteFileHandler
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
        zarr_store_handler = ZarrRemoteFileHandler(job, env.SOURCE_PATH)
        zarr_store_handler.async_download()

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
            job_handler.ack()
            return

        logger.info(f"generating outputs for job. job: {job}")
        cocip_handler = CocipHandler(
            zarr_store_handler.local_zarr_store_fp,
            job,
            f"{env.SINK_PATH}/grids",
            f"{env.SINK_PATH}/regions",
        )
        cocip_handler.read()
        cocip_handler.compute()
        cocip_handler.write()
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
        except ZarrRemoteFileHandler as e:
            logger.error(f"{e}. traceback: {format_traceback()}")
            sys.exit(1)
        except Exception:
            logger.error("Unhandled exception:" + format_traceback())
            sys.exit(1)
