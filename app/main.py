"""Entrypoint for the (internal) API hosting API Preprocessor methods."""

import flask

from app.schemas import ApiPreprocessorJob
from app.handlers import CocipHandler

from app.log import logger

app = flask.Flask(__name__)


@app.route("/run", methods=["POST"])
def run() -> tuple[str, int]:
    """
    Generate grids and regions data product,
    and write it to a location backing the /v1 public API.

    This service  consumes jobs form Pubsub.
    Each payload containing a JSON representation of an APIPreprocessorJob.

    The HRES ETL service is responsible for generating and publishing API Preprocessor jobs.
    """
    req = flask.request.get_json()  # noqa:F841
    logger.info("call /run")
    # TODO: fetch job from subscription
    # TODO: init background daemon handling ack extension/lease management
    # TODO: extract job attributes from payload

    # stubbed values
    # -------
    job = ApiPreprocessorJob(
        model_run_at=1708322400,
        model_predicted_at=1708354800,
        flight_level=300,
        aircraft_class="default",
    )

    cocip_handler = CocipHandler(
        "gs://contrails-301217-ecmwf-hres-forecast-v2-short-term",
        job,
        "gs://contrails-301217-api-preprocessor-dev/grids",
        "gs://contrails-301217-api-preprocessor-dev/regions",
    )
    cocip_handler.read()
    cocip_handler.compute()
    cocip_handler.write()

    logger.info("processing of job complete.  message id: foobar")
    return "success", 200


if __name__ == "__main__":
    app.run(threaded=True, host="0.0.0.0", port=8080)
