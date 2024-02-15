"""Entrypoint for the (internal) API hosting API Preprocessor methods."""

from random import randint

import flask

from app.log import logger

app = flask.Flask(__name__)


@app.route("/", methods=["GET"])
def health_check() -> tuple[str, int]:
    """Check if the app is running."""
    logger.info("Call health check /")
    return "success", 200


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

    # TODO: fetch job from subscription
    # TODO: init background daemon handling ack extension/lease management
    # TODO: extract job attributes from payload

    # stubbed values
    # -------
    # input params
    aircraft_class = "default"  # noqa:F841 please help to enum all of these
    flight_level = 300
    model_run_at = 1707890400
    model_predicted_at = 1707926400
    polygon_thresholds = [500000000, 5000000, 50000]

    # helpers
    offset_hrs = (model_predicted_at - model_run_at) // 60
    roll_em = randint(0, 100)

    # output paths
    grids_gcs_sink_path = (  # noqa:F841
        f"gs://contrails-301217-api-preprocessor-dev/"
        f"grids/{aircraft_class}/{model_predicted_at}_{flight_level}/{offset_hrs}-{roll_em}.nc"
    )
    regions_gcs_sink_path = [  # noqa:F841
        (
            f"gs://contrails-301217-api-preprocessor-dev/"
            f"regions/{aircraft_class}/{model_predicted_at}_{flight_level}/"
            f"{offset_hrs}-{roll_em}/{thres}.geojson"
        )
        for thres in polygon_thresholds
    ]

    # TODO: build cocip grid at 0.25deg x 0.25deg, export to gcs as netcdf file
    # TODO: build polygon files for each threshold, export gcs as geojson

    logger.info("processing of job complete.  message id: foobar")
    return "success", 200


if __name__ == "__main__":
    app.run(threaded=True, host="0.0.0.0", port=8080)
