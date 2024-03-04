"""
Application handlers.
"""

import time
from typing import Union

from lib.schemas import ApiPreprocessorJob
from lib.log import logger, format_traceback
from datetime import datetime, timezone, timedelta

from google.api_core import retry
from google.cloud import pubsub_v1
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
from threading import Thread
import warnings


class CocipHandler:
    """
    Handler for managing ingress of HRES data, cocip data product rendering, and data egress.
    """

    # TODO: finalize values
    PROVISIONAL_STATIC_PARAMS = dict(
        humidity_scaling=ExponentialBoostLatitudeCorrectionHumidityScaling(),
        dt_integration="5min",
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

    REGIONS_THRESHOLDS = [5e8, 1e8, 1e7]
    MAX_AGE_HR = 12

    def __init__(
        self,
        hres_source_path: str,
        job: ApiPreprocessorJob,
        grids_sink_path: str,
        regions_sink_path: str,
    ):
        """
        Parameters
        ----------
        hres_source_path
            fully-qualified base path reading HRES zarr store input
            e.g. 'gs://contrails-301217-ecmwf-hres-forecast-v2-short-term'
        job
            the API Preprocessor job to be processed by the handler
        grids_sink_path
            fully-qualified base path for writing cocip grid netcdf output
            e.g. 'gs://contrails-301217-api-preprocessor-dev/grids'
        regions_sink_path
            fully-qualified base path for writing cocip regions geojson output
            e.g. 'gs://contrails-301217-api-preprocessor-dev/regions'
        """
        self._hres_datasets: Union[None, tuple[MetDataset, MetDataset]] = None
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

        offset_hrs = (job.model_predicted_at - job.model_run_at) // 3600
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
        self._hres_datasets = self._load_met_rad(self._run_at_dt, self.hres_source_path)

    def compute(self):
        """
        Compute the cocip grid.
        """
        if not self._hres_datasets:
            raise ValueError(
                "missing input data. please run read() to load input data."
            )

        source = self._create_cocip_grid_source(
            self._predicted_at_dt,
            self.job.flight_level,
        )
        model = self._create_cocip_grid_model(
            *self._hres_datasets, self.job.aircraft_class
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
            self._cocip_grid.data, self.grids_gcs_sink_path
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


class JobSubscriptionHandler:
    """
    Handler for managing consumption and marshalling of jobs from a pubsub subscription queue.
    """

    # the number of seconds the subscriber client will hang, waiting for available messages
    MSG_WAIT_TIME_SEC = 60.0

    def __init__(self, subscription: str, ack_extension_sec: int):
        """
        Parameters
        ----------
        subscription
            The fully-qualified URI for the pubsub subscription.
            e.g. 'projects/contrails-301217/subscriptions/api-preprocessor-sub-dev'
        ack_extension_sec
            This handler will indefinitely extend the active message's ack deadline by
            ack_extension_sec until self.ack() is called
        """
        self.subscription = subscription
        self.ack_extension_sec = ack_extension_sec
        self._client = None
        self._ack_id: Union[None, str] = None
        self._kill_ack_manager = False
        self._ack_manager = Thread(target=self._ack_management_worker, daemon=True)
        self._ack_manager.start()

    def __enter__(self):
        """
        Initialize pubsub client to be used across this class instance's lifecycle.
        """
        self._client = pubsub_v1.SubscriberClient()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Ensure client connection to pubsub is closed.
        """
        self.close()

    def _ack_management_worker(self):
        """
        Extends the ack deadline for the currently outstanding message.
        """
        logger.info("starting ack lease management worker...")
        while not self._kill_ack_manager:
            time.sleep(self.ack_extension_sec // 2)
            if self._ack_id:
                logger.info(
                    f"extending ack deadline on ack_id: {self._ack_id[0:-40]}..."
                )
                try:
                    self._client.modify_ack_deadline(
                        request={
                            "subscription": self.subscription,
                            "ack_ids": [self._ack_id],
                            "ack_deadline_seconds": self.ack_extension_sec,
                        }
                    )
                except Exception:
                    logger.error(
                        f"failed to extend ack deadline for message. "
                        f"traceback: {format_traceback()}"
                    )

    def fetch(self) -> ApiPreprocessorJob:
        """
        Fetch a message from the subscription queue.
        This method will hang and wait until a message is available.
        This method, in case of exception, will hang, backoff and retry indefinitely.

        Returns
        -------
        str
            The dequeued message from the pubsub subscription.
        """
        if not self._client:
            self._client = pubsub_v1.SubscriberClient()
            warnings.warn(
                "pubsub subscriber client initialized. "
                "connection will remain open until close()."
            )

        while True:
            logger.info(f"fetching message from {self.subscription}")
            resp = self._client.pull(
                request={"subscription": self.subscription, "max_messages": 1},
                retry=retry.Retry(timeout=30.0),
                timeout=self.MSG_WAIT_TIME_SEC,
            )

            if len(resp.received_messages) == 0:
                # it is possible there are no messages available,
                # or, pubsub returned zero when there are in fact some messages to fetch on retry
                logger.info("zero messages received.")
                continue
            msg = resp.received_messages[0]
            self._ack_id = msg.ack_id
            logger.info(
                f"received 1 message from {self.subscription}. "
                f"published_time: {msg.message.publish_time}, "
                f"message_id: {msg.message.message_id}"
            )
            return ApiPreprocessorJob.from_utf8_json(msg.message.data)

    def ack(self):
        """
        Acknowledge the outstanding message presently handled by the instance of this class.
        """
        if not self._ack_id:
            raise ValueError(
                "ack_id is not set. call fetch(). "
                "handler instance must be handling an outstanding message."
            )
        self._client.acknowledge(
            request={"subscription": self.subscription, "ack_ids": [self._ack_id]},
            retry=retry.Retry(timeout=30.0),
        )
        logger.info("successfully ack'ed message.")
        self._ack_id = None

    def close(self):
        """
        Close pubsub client connection.
        """
        self._kill_ack_manager = True
        self._ack_manager.join()
        self._client.close()
