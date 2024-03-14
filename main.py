"""Entrypoint for the (internal) API hosting API Preprocessor methods."""

from lib.handlers import CocipHandler, JobSubscriptionHandler
import lib.environment as env
from lib.log import logger
from lib.schemas import ApiPreprocessorJob


def run():
    """
    Generate grids and regions data product,
    and write it to a location backing the /v1 public API.

    This service  consumes jobs form Pubsub.
    Each payload containing a JSON representation of an APIPreprocessorJob.

    The HRES ETL service is responsible for generating and publishing API Preprocessor jobs.
    """
    logger.info("initiating run()")
    with JobSubscriptionHandler(env.API_PREPROCESSOR_SUBSCRIPTION_ID) as _:
        # job = job_handler.fetch()

        # stubbed values
        # -------
        job = ApiPreprocessorJob(
            model_run_at=1710244800,  # 2024-03-12T12
            model_predicted_at=1710248400,  # 2024-03-12T13
            flight_level=300,
            aircraft_class="default",
        )
        logger.info(f"generating outputs for job. job: {job}")
        cocip_handler = CocipHandler(
            env.SOURCE_PATH,
            job,
            f"{env.SINK_PATH}/grids",
            f"{env.SINK_PATH}/regions",
        )
        cocip_handler.read()
        cocip_handler.compute()
        cocip_handler.write()
        # job_handler.ack()
    logger.info(f"processing of job complete. job: {job}")


if __name__ == "__main__":
    logger.info("starting api-preprocessor instance")
    while True:
        run()
