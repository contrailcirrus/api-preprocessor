"""Entrypoint for the (internal) API hosting API Preprocessor methods."""

import datetime
import flask
import numpy as np
import xarray as xr
import gcsfs

from pycontrails import MetDataset
from pycontrails.models.cocipgrid import CocipGrid
from pycontrails.models.ps_model import PSGrid
from pycontrails.models.humidity_scaling import ExponentialBoostLatitudeCorrectionHumidityScaling
from pycontrails.physics import units

from app.log import logger

app = flask.Flask(__name__)

# TODO: finalize values
PROVISIONAL_COCIP_GRID_PARAMS = dict(
    humidity_scaling=ExponentialBoostLatitudeCorrectionHumidityScaling(),
    dt_integration= "5min",
    max_age="12h",
    met_slice_dt="1h",
    target_split_size=100_000,
    target_split_size_pre_SAC_boost=2.5,
    max_altitude_m=None,
    min_altitude_m=None,
    azimuth=None,
    segment_length=None,
    dsn_dz_factor=0.665,
    interpolation_use_indices=True,
    interpolation_bounds_error=True,
    show_progress=False,
    filter_sac=True,
    copy_source=True
)

@app.route("/", methods=["GET"])
def health_check() -> tuple[str, int]:
    """Check if the app is running."""
    logger.info("Call health check /")
    return "success", 200

def _load_met_rad(t: datetime.datetime) -> tuple[MetDataset, MetDataset]:
    bucket = "gs://contrails-301217-ecmwf-hres-forecast-v2-short-term"
    forecast = t.strftime("%Y%m%d%H")
    
    pl = xr.open_zarr(f"{bucket}/{forecast}/pl.zarr/")
    met = MetDataset(pl, provider="ECMWF", dataset="HRES", product="forecast")
    variables = (v[0] if isinstance(v, tuple) else v for v in CocipGrid.met_variables)
    met.standardize_variables(variables)
    
    sl = xr.open_zarr(f"{bucket}/{forecast}/sl.zarr/")
    rad = MetDataset(sl, provider="ECMWF", dataset="HRES", product="forecast")
    variables = (v[0] if isinstance(v, tuple) else v for v in CocipGrid.rad_variables)
    rad.standardize_variables(variables)
    return met, rad


def _create_cocip_grid_source(t: datetime.datetime, flight_level: int) -> MetDataset:
    hor_res = 0.25
    dtype = np.float64
    longitude = np.arange(-180, 180, hor_res, dtype=dtype)
    latitude = np.arange(-80, 80.01, hor_res, dtype=dtype)
    altitude_ft = flight_level * 100.0
    level = units.ft_to_pl(altitude_ft)
    return MetDataset.from_coords(
        longitude=longitude,
        latitude=latitude,
        level=level,
        time=t,
    )

def _create_cocip_grid_model(met: MetDataset, rad: MetDataset, aircraft_class: str) -> CocipGrid:
    # TODO: set relevant parameters based on aircraft class
    return CocipGrid(
        met=met,
        rad=rad,
        aircraft_performance=PSGrid(),
        **PROVISIONAL_COCIP_GRID_PARAMS
    )
    

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
    model_run_at = 1708318800
    model_predicted_at = 1708354800
    polygon_thresholds = [500000000, 5000000, 50000]
    max_max_age_hr = 12  # noqa:F841

    # helpers
    offset_hrs = (model_predicted_at - model_run_at) // 60

    # output paths
    grids_gcs_sink_path = (  # noqa:F841
        f"gs://contrails-301217-api-preprocessor-dev/"
        f"grids/{aircraft_class}/{model_predicted_at}_{flight_level}/{offset_hrs}.nc"
    )
    regions_gcs_sink_path = [  # noqa:F841
        (
            f"gs://contrails-301217-api-preprocessor-dev/"
            f"regions/{aircraft_class}/{model_predicted_at}_{flight_level}/"
            f"{offset_hrs}/{thres}.geojson"
        )
        for thres in polygon_thresholds
    ]

    # TODO: build cocip grid at 0.25deg x 0.25deg, export to gcs as netcdf file
    run_at = datetime.datetime.fromtimestamp(model_run_at)
    pred_at = datetime.datetime.fromtimestamp(model_predicted_at)
    max_age = min(datetime.timedelta(hours=max_max_age_hr), run_at + datetime.timedelta(hours=72) - pred_at)

    met, rad = _load_met_rad(run_at)
    source = _create_cocip_grid_source(pred_at, flight_level)
    model = _create_cocip_grid_model(met, rad, aircraft_class)

    result = model.eval(source)
    result.data.to_netcdf(...)

    fs = gcsfs.GCSFileSystem()
    with fs.open(grids_gcs_sink_path) as f:
        result.data.to_netcdf(f)

    # save w/ gcsfs

    # TODO: build polygon files for each threshold, export gcs as geojson

    logger.info("processing of job complete.  message id: foobar")
    return "success", 200


if __name__ == "__main__":
    app.run(threaded=True, host="0.0.0.0", port=8080)
