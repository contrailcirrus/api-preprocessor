"""
Application handlers.
"""

from typing import Union

from app.schemas import ApiPreprocessorJob
from datetime import datetime, timezone, timedelta

from pycontrails import MetDataset
from pycontrails.models.cocipgrid import CocipGrid
from pycontrails.models.ps_model import PSGrid
from pycontrails.models.humidity_scaling import (
    ExponentialBoostLatitudeCorrectionHumidityScaling,
)

from pycontrails.physics import units

import numpy as np
import xarray as xr
import gcsfs
import tempfile


class CocipHandler:
    """
    Handler for managing ingress of HRES data, cocip data product rendering, and data egress.
    """

    # TODO: finalize values
    PROVISIONAL_STATIC_PARAMS = dict(
        humidity_scaling=ExponentialBoostLatitudeCorrectionHumidityScaling(),
        dt_integration="5min",
        met_slice_dt="1h",
        target_split_size=100_000,
        target_split_size_pre_SAC_boost=2.5,
        max_altitude_m=None,
        min_altitude_m=None,
        azimuth=None,
        segment_length=None,
        dsn_dz_factor=0.665,
        interpolation_use_indices=True,
        interpolation_bounds_error=False,
        show_progress=False,
        filter_sac=True,
        copy_source=True,
        met_level_buffer=(20, 20),
    )

    REGIONS_THRESHOLDS = [500000000, 5000000, 50000]
    MAX_AGE_HR = 12

    def __init__(
        self,
        hres_source_path: str,
        job: ApiPreprocessorJob,
        grids_sink_path: str,
        regions_sink_path: str,
    ):
        """
        :param hres_source_path: fully-qualified base path reading HRES zarr store input
                                 e.g. 'gs://contrails-301217-ecmwf-hres-forecast-v2-short-term'
        :param job: the API Preprocessor job to be processed by the handler
        :param grids_sink_path: fully-qualified base path for writing cocip grid netcdf output
                               e.g. 'gs://contrails-301217-api-preprocessor-dev/grids'
        :param regions_sink_path: fully-qualified base path for writing cocip regions geojson output
                               e.g. 'gs://contrails-301217-api-preprocessor-dev/regions'
        """
        self._hres_dataset: Union[None, tuple[MetDataset, MetDataset]] = None
        self._cocip_grid: Union[None, MetDataset] = None

        self.hres_source_path = hres_source_path
        self.job = job

        self._run_at_dt = datetime.fromtimestamp(job.model_run_at, tz=timezone.utc)
        self._predicted_at_dt = datetime.fromtimestamp(
            job.model_predicted_at, tz=timezone.utc
        )
        self._max_age = min(
            timedelta(hours=self.MAX_AGE_HR),
            self._run_at_dt + timedelta(hours=72) - self._predicted_at_dt,
        )

        offset_hrs = (job.model_predicted_at - job.model_run_at) // 60
        self.grids_gcs_sink_path = (
            f"{grids_sink_path}/{job.aircraft_class}"
            f"/{job.model_predicted_at}_{job.flight_level}/{offset_hrs}.nc"
        )
        self.regions_gcs_sink_path = [
            (
                f"{regions_sink_path}"
                f"/{job.aircraft_class}/{job.model_predicted_at}_{job.flight_level}/"
                f"{offset_hrs}/{thres}.geojson"
            )
            for thres in self.REGIONS_THRESHOLDS
        ]

    def read(self):
        """
        Extract hres inputs from zarr store, load to memory.
        """
        self._hres_dataset = self._load_met_rad(self._run_at_dt, self.hres_source_path)

    def compute(self):
        """
        Compute the cocip grid.
        """
        if not self._hres_dataset:
            raise ValueError(
                "missing input data. please run read() to load input data."
            )

        source = self._create_cocip_grid_source(
            self._predicted_at_dt,
            self.job.flight_level,
        )
        model = self._create_cocip_grid_model(
            *self._hres_dataset, self.job.aircraft_class
        )
        result = model.eval(source, max_age=self._max_age)
        self._fix_attrs(
            result
        )  # serialization as netcdf fails if any attributes are None,
        self._cocip_grid = result

    def write(self):
        """
        Write the generated data products to storage.
        """
        if not self._cocip_grid:
            raise ValueError(
                "missing cocip grid data. please run compute() to generate model output data."
            )

        # TODO: generate geojson regions/polygons, and write to file
        self._save_nc4(
            self._cocip_grid, self.grids_gcs_sink_path
        )  # complicated---see comments in helper function

    @staticmethod
    def _load_met_rad(
        t: datetime, hres_source_path: str
    ) -> tuple[MetDataset, MetDataset]:
        # NOTE: this bucket contains 0.25 x 0.25 degree HRES data
        # full-resolution (0.1 x 0.1 degree) HRES data is in
        # gs://contrails-301217-ecmwf-hres-forecast-v2-short-term-dev
        forecast = t.strftime("%Y%m%d%H")

        pl = xr.open_zarr(f"{hres_source_path}/{forecast}/pl.zarr/")
        met = MetDataset(pl, provider="ECMWF", dataset="HRES", product="forecast")
        variables = (
            v[0] if isinstance(v, tuple) else v for v in CocipGrid.met_variables
        )
        met.standardize_variables(variables)

        sl = xr.open_zarr(f"{hres_source_path}/{forecast}/sl.zarr/")
        rad = MetDataset(sl, provider="ECMWF", dataset="HRES", product="forecast")
        variables = (
            v[0] if isinstance(v, tuple) else v for v in CocipGrid.rad_variables
        )
        rad.standardize_variables(variables)
        return met, rad

    @staticmethod
    def _create_cocip_grid_source(t: datetime, flight_level: int) -> MetDataset:
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

    @classmethod
    def _create_cocip_grid_model(
        cls, met: MetDataset, rad: MetDataset, aircraft_class: str
    ) -> CocipGrid:
        # TODO: set relevant parameters based on aircraft class
        # Logic for setting per-class parameters should probably live in pycontrails,
        # but doesn't exist yet
        return CocipGrid(
            met=met,
            rad=rad,
            aircraft_performance=PSGrid(),
            **cls.PROVISIONAL_STATIC_PARAMS,
        )

    @staticmethod
    def _fix_attrs(result: MetDataset) -> None:
        for key, value in result.data.attrs.items():
            if value is None:
                result.data.attrs[key] = "None"

    @staticmethod
    def _save_nc4(ds: xr.Dataset, sink_path: str) -> None:
        # Can only save as netcdf3 with file-like objects:
        # https://docs.xarray.dev/en/stable/generated/xarray.Dataset.to_netcdf.html.
        # We want netcdf4, so have to save to a temporary file using its path,
        # then transfer the result to the cloud bucket
        with tempfile.NamedTemporaryFile(delete_on_close=False) as tmp:
            tmp.close()
            ds.to_netcdf(tmp.name, format="NETCDF4")
            fs = gcsfs.GCSFileSystem()
            fs.put(tmp.name, sink_path)
