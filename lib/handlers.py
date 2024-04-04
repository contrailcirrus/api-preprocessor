"""
Application handlers.
"""

import os
import time
from typing import Union

from lib.schemas import ApiPreprocessorJob
from lib.log import logger, format_traceback
from lib.exceptions import QueueEmptyError, ZarrStoreDownloadError
from datetime import datetime, timezone, timedelta

from google.api_core import retry
from google.cloud import pubsub_v1
from google.cloud.storage import Client, transfer_manager
from pycontrails import MetDataset, MetDataArray
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
import geojson
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
        self._polygons: Union[None, list[geojson.FeatureCollection]] = None

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
        logger.info("composing cocip grid model.")
        model = self._create_cocip_grid_model(
            *self._hres_datasets, self.job.aircraft_class
        )
        logger.info("evaluating cocip grid model.")
        result = model.eval(source, max_age=self._max_age)
        self._fix_attrs(
            result
        )  # serialization as netcdf fails if any attributes are None,
        self._cocip_grid = result
        logger.info("done evaluating cocip grid model.")

        polys = [
            self._build_polygons(result["ef_per_m"], thres)
            for thres in self.REGIONS_THRESHOLDS
        ]
        self._polygons = polys

    def write(self):
        """
        Write the generated data products to storage.
        """
        if not self._cocip_grid:
            raise ValueError(
                "missing cocip grid data. please run compute() to generate model output data."
            )
        if not self._polygons:
            raise ValueError(
                "missing polygon data. please run compute() to generate polygons"
            )

        self._save_nc4(
            self._cocip_grid.data, self.grids_gcs_sink_path
        )  # complicated---see comments in helper function

        for poly, path in zip(self._polygons, self.regions_gcs_sink_path):
            self._save_geojson(poly, path)

    @staticmethod
    def _load_met_rad(
        t: datetime, hres_source_path: str
    ) -> tuple[MetDataset, MetDataset]:
        # NOTE: this bucket contains 0.25 x 0.25 degree HRES data
        # full-resolution (0.1 x 0.1 degree) HRES data is in
        # gs://contrails-301217-ecmwf-hres-forecast-v2-short-term-dev
        forecast = t.strftime("%Y%m%d%H")

        pl_fp = f"{hres_source_path}/{forecast}/pl.zarr/"
        logger.info(f"opening pressure level zarr file: {pl_fp}")
        pl = xr.open_zarr(pl_fp)
        met = MetDataset(pl, provider="ECMWF", dataset="HRES", product="forecast")
        variables = (
            v[0] if isinstance(v, tuple) else v for v in CocipGrid.met_variables
        )
        met.standardize_variables(variables)

        sl_fp = f"{hres_source_path}/{forecast}/sl.zarr/"
        sl = xr.open_zarr(sl_fp)
        logger.info(f"opening surface level zarr file: {sl_fp}")
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

    @staticmethod
    def _build_polygons(
        ef_per_m: MetDataArray, threshold: int
    ) -> geojson.FeatureCollection:
        # parameters for building polygons are defaults from /v0 API; see
        # https://github.com/contrailcirrus/contrails-api/blob/bd8b0a8a858be2852346c35316c7cdc96ac65a2f/app/schemas.py
        # https://github.com/contrailcirrus/contrails-api/blob/bd8b0a8a858be2852346c35316c7cdc96ac65a2f/app/settings.py
        # https://github.com/contrailcirrus/contrails-api/blob/bd8b0a8a858be2852346c35316c7cdc96ac65a2f/app/v0/grid.py
        #
        # For descriptions of parameters, see
        # https://py.contrails.org/api/pycontrails.core.met.html#pycontrails.core.met.MetDataArray.to_polygon_feature
        params = dict(
            fill_value=0.0,  # grid.py L602
            iso_value=threshold,  # polygon threshold set by `threshold` parameter
            min_area=0.3,  # schemas.py L1396, used to index `POLYGON_MIN_AREA` in settings.py
            epsilon=0.05,  # schemas.py L1396, used to index `POLYGON_EPSILON` in settings.py
            precision=2,  # `POLYGON_PRECISION` in settings.py
            interiors=True,  # schemas.py L1378
            convex_hull=False,  # schemas.py L1417
            include_altitude=True,  # grid.py L601
        )
        logger.info(f"building polygon for threshold: {threshold}")
        poly = ef_per_m.to_polygon_feature(**params)
        return geojson.FeatureCollection(poly)

    @staticmethod
    def _save_geojson(fc: geojson.FeatureCollection, sink_path: str) -> None:
        fs = gcsfs.GCSFileSystem()
        with fs.open(sink_path, "w") as f:
            geojson.dump(fc, f)


class JobSubscriptionHandler:
    """
    Handler for managing consumption and marshalling of jobs from a pubsub subscription queue.
    """

    # the number of seconds the subscriber client will hang, waiting for available messages
    MSG_WAIT_TIME_SEC = 60.0
    ACK_EXTENSION_SEC: int = 300

    def __init__(self, subscription: str):
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
            time.sleep(self.ACK_EXTENSION_SEC // 2)
            if self._ack_id:
                logger.info(
                    f"extending ack deadline on ack_id: {self._ack_id[0:-150]}..."
                )
                try:
                    self._client.modify_ack_deadline(
                        request={
                            "subscription": self.subscription,
                            "ack_ids": [self._ack_id],
                            "ack_deadline_seconds": self.ACK_EXTENSION_SEC,
                        }
                    )
                except Exception:
                    logger.error(
                        f"failed to extend ack deadline for message. "
                        f"traceback: {format_traceback()}"
                    )
        logger.info("terminated ack lease management worker")

    def fetch(self) -> ApiPreprocessorJob:
        """
        Fetch a message from the subscription queue.
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
            raise QueueEmptyError()

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
        self._client.close()


class ZarrRemoteFileHandler:
    """
    Handler for managing localization of remote GCS dirs/files.
    """

    TMPDIR = "/tmp"
    DOWNLOAD_THREAD_CNT = 10

    def __init__(self, job: ApiPreprocessorJob, source: str):
        """
         Parameters
         ----------
        job
             an ApiPreprocessorJob instance, from which we identify the requisite zarr store
         source
             the remote GCS bucket URI for fetching the zarr store
             e.g. "gs://contrails-301217-ecmwf-hres-forecast-v2-short-term-dev"
        """
        self._src_bucket = source.split("/")[2]
        self._zarr_store_dir_name = datetime.fromtimestamp(
            job.model_run_at,
            tz=timezone.utc,
        ).strftime("%Y%m%d%H")

    def local_store_exists(self) -> bool:
        return self._zarr_store_dir_name in os.listdir(self.TMPDIR)

    def async_download(self):
        """
        Concurrently fetch all files (chunks and metadata) from a GCS remote zarr store.
        adapted from:
            https://cloud.google.com/storage/docs/downloading-objects#downloading-an-object
        """
        if self.local_store_exists():
            logger.info("zarr store already exists locally. not downloading.")
            return

        storage_client = Client()
        bucket = storage_client.bucket(self._src_bucket)

        blob_names = [
            blob.name for blob in bucket.list_blobs(prefix=self._zarr_store_dir_name)
        ]

        results = transfer_manager.download_many_to_path(
            bucket,
            blob_names,
            destination_directory=self.TMPDIR,
            worker_type=transfer_manager.THREAD,
            max_workers=self.DOWNLOAD_THREAD_CNT,
        )

        for name, result in zip(blob_names, results):
            # The results list is either `None` or an exception for each blob in
            # the input list, in order.
            if isinstance(result, Exception):
                raise ZarrStoreDownloadError(
                    "failed to download {} due to exception: {}".format(name, result)
                )
        logger.info(f"zarr store downloaded to {self.local_zarr_store_fp}")

    @property
    def local_zarr_store_fp(self):
        return f"{self.TMPDIR}/{self._zarr_store_dir_name}"
