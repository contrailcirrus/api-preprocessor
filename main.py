"""Entrypoint for the (internal) API hosting API Preprocessor methods."""

from lib.handlers import CocipHandler, JobSubscriptionHandler
import lib.environment as env
from lib.log import logger


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
        job_handler.ack()

    # stubbed values
    # -------
    # job = ApiPreprocessorJob(
    #    model_run_at=1708322400,
    #    model_predicted_at=1708354800,
    #    flight_level=300,
    #    aircraft_class="default",
    # )

    # TEMPORARY
    # lets sip off our queue for starters
    from random import randint

    if randint(0, 100) != 20:
        return

    logger.info(f"generating outputs for job. job: {job}")
    cocip_handler = CocipHandler(
        "gs://contrails-301217-ecmwf-hres-forecast-v2-short-term-dev",
        job,
        "gs://contrails-301217-api-preprocessor-dev/grids",
        "gs://contrails-301217-api-preprocessor-dev/regions",
    )
    cocip_handler.read()
    cocip_handler.compute()
    cocip_handler.write()

    # TODO: move cocip work w/in job_handler context
    #  add pubsub message ack after confirming successful data output write
    logger.info(f"processing of job complete. job: {job}")


if __name__ == "__main__":
    logger.info("starting api-preprocessor instance")
    while True:
        run()
