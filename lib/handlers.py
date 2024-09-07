"""
Application handlers.
"""

import concurrent.futures
import json
import os
import tempfile
import threading
from collections.abc import Callable, Iterator

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

import dask
import gcsfs  # type: ignore
import geojson  # type: ignore
import google.api_core.exceptions
import google.api_core.retry
import numpy as np
import xarray as xr
from google.cloud import pubsub_v1  # type: ignore
from google.cloud.storage import Client, transfer_manager  # type: ignore
from pycontrails import MetDataArray, MetDataset
from pycontrails.models.cocipgrid import CocipGrid
from pycontrails.models.humidity_scaling import (
    ExponentialBoostLatitudeCorrectionHumidityScaling,
)
from pycontrails.models.ps_model import PSGrid
from pycontrails.physics import units

from lib.exceptions import QueueEmptyError, ZarrStoreDownloadError
from lib.log import format_traceback, logger
from lib.schemas import ApiPreprocessorJob

# oversubscribe dask workers
pool = ThreadPoolExecutor(8)
dask.config.set(pool=pool)


class ValidationHandler:
    """
    A validation handler that guarantees certain CoCip model prerequisites are met.
    Tests are applied to the HRES data and zarr stores.
    """

    def __init__(self, job: ApiPreprocessorJob):
        self._job = job

    def sufficient_forecast_range(self) -> bool:
        """
        Verifies that sufficient HRES data exists to run the CoCip model.
        Loosely, this is governed by the MAX_AGE_HR configuration on the CoCip model instance.

        Specifically, the current (0.49.3) pycontrails implementation needs a buffer
        for differencing accumulated radiative fluxes.
        Ergo, conditions:
        1) job.model_predicted_at must be at least half an hour after job.model_run_at
        2) prediction_runway_hrs must extend at least half an hour beyond CocipHandler.MAX_AGE_HR
        """
        # 72 hrs of fwd met data per model run
        prediction_wall = self._job.model_run_at + 72 * 60 * 60

        prediction_runway_hrs = (prediction_wall - self._job.model_predicted_at) / (
            60 * 60
        )
        if (
            self._job.model_predicted_at
            < self._job.model_run_at + 1800  # seconds in 0.5 hr
            or prediction_runway_hrs < CocipHandler.MAX_AGE_HR + 0.5
        ):
            return False
        return True


class CocipHandler:
    """
    Handler for managing ingress of HRES data, cocip data product rendering, and data egress.
    """

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

    REGIONS_THRESHOLDS = [-1, 1, 1e7, 2.5e7, 5e7, 7.5e7, 1e8, 2.5e8, 5e8, 7.5e8, 1e9]
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
        self._hres_datasets: None | tuple[MetDataset, MetDataset] = None
        self._cocip_grid: None | MetDataset = None
        self._polygons: dict[int, dict[int, geojson.FeatureCollection]] = dict()

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

        # {<fl>: <gcs_path>}
        self.grids_gcs_sink_paths: dict[int, str] = dict()
        # {<fl>: {<thres>: <gcs_path>}}
        self.regions_gcs_sink_paths: dict[int, dict[int, str]] = dict()

        if self.job.flight_level == ApiPreprocessorJob.ALL_FLIGHT_LEVELS_WILDCARD:
            for fl in ApiPreprocessorJob.FLIGHT_LEVELS:
                self.grids_gcs_sink_paths.update(
                    {
                        fl: f"{grids_sink_path}/{job.aircraft_class}/"
                        f"{job.model_predicted_at}_{fl}/{offset_hrs}.nc"
                    }
                )
                self.regions_gcs_sink_paths.update({fl: dict()})
                for thres in self.REGIONS_THRESHOLDS:
                    self.regions_gcs_sink_paths[fl].update(
                        {
                            thres: f"{regions_sink_path}/{job.aircraft_class}/"
                            f"{job.model_predicted_at}_{fl}/"
                            f"{offset_hrs}/{thres:.0f}.geojson"
                        }
                    )
        else:
            self.grids_gcs_sink_paths.update(
                {
                    job.flight_level: f"{grids_sink_path}/{job.aircraft_class}/"
                    f"{job.model_predicted_at}_{job.flight_level}/{offset_hrs}.nc"
                }
            )
            self.regions_gcs_sink_paths.update({job.flight_level: dict()})
            for thres in self.REGIONS_THRESHOLDS:
                self.regions_gcs_sink_paths[job.flight_level].update(
                    {
                        thres: f"{regions_sink_path}/{job.aircraft_class}/"
                        f"{job.model_predicted_at}_{job.flight_level}/"
                        f"{offset_hrs}/{thres:.0f}.geojson"
                    }
                )

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

        for fl in self.regions_gcs_sink_paths.keys():
            self._polygons.update({fl: dict()})
            for thres in self.regions_gcs_sink_paths[fl].keys():
                level = units.ft_to_pl(fl * 100.0)
                poly = self._build_polygons(
                    result.data.sel(level=[level])["ef_per_m"],
                    thres,
                )
                self._polygons[fl].update({thres: poly})

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

        for fl, gcs_grid_path in self.grids_gcs_sink_paths.items():
            # TODO: remove logging
            logger.info(f"writing grid to: {gcs_grid_path}")
            self._save_nc4(
                self._cocip_grid.data.sel(level=[units.ft_to_pl(fl * 100.0)]),
                gcs_grid_path,
                fl,
            )  # complicated---see comments in helper function

        for fl, thres_lookup in self.regions_gcs_sink_paths.items():
            for thres, gcs_region_path in thres_lookup.items():
                logger.info(f"writing geojson to: {gcs_region_path}")
                self._save_geojson(self._polygons[fl][thres], gcs_region_path)

    @property
    def regions(self) -> None | dict[int, dict[int, str]]:
        """
        Response contains a mapping {<fl>: {<thres>: <poly>}},
        where fl and thres are flight level and threshold, respectively.
        The <poly> is a string representation of a geojson MultiPolygon object,
        which itemizes all CoCip polygons for the given flight level .

        Altitude (third coordinate position) is removed from the points in the MultiPolygon.
        Three positional elements are not supported in BigQuery
        ref (Constraints): https://cloud.google.com/bigquery/docs/reference/standard-sql/geography_functions#st_geogfromgeojson
        """
        if not self._polygons:
            return None

        fl: int
        thres: int
        poly: geojson.FeatureCollection
        out: dict[int, dict[int, str]] = dict()
        for fl, thres_lookup in self._polygons.items():
            for thres, poly in thres_lookup.items():
                feature_geom = dict(poly.features[0]["geometry"])
                # remove third positional element in each point (lon, lat, alt)
                for polygon in feature_geom["coordinates"]:
                    for linestring in polygon:
                        for coord in linestring:
                            coord.pop()
                feature_str = json.dumps(feature_geom)
                out.update({fl: {thres: feature_str}})
        return out

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

    def _create_cocip_grid_source(self, t: datetime, flight_level: int) -> MetDataset:
        hor_res = 0.25
        dtype = np.float64
        longitude = np.arange(-180, 180, hor_res, dtype=dtype)
        latitude = np.arange(-80, 80.01, hor_res, dtype=dtype)
        if flight_level == ApiPreprocessorJob.ALL_FLIGHT_LEVELS_WILDCARD:
            altitude_ft = [fl * 100.0 for fl in ApiPreprocessorJob.FLIGHT_LEVELS]
            level = [units.ft_to_pl(alt_ft) for alt_ft in altitude_ft]
            return MetDataset.from_coords(
                longitude=longitude,
                latitude=latitude,
                level=level,
                time=t,
            )
        else:
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

        aircraft_attrs = ApiPreprocessorJob.AIRCRAFT_CLASSES[aircraft_class]

        return CocipGrid(
            met=met,
            rad=rad,
            aircraft_performance=PSGrid(),
            aircraft_type=aircraft_attrs["aircraft_type_icao"],
            engine_uid=aircraft_attrs["engine_uid"],
            **cls.PROVISIONAL_STATIC_PARAMS,
        )

    @staticmethod
    def _fix_attrs(result: MetDataset) -> None:
        for key, value in result.data.attrs.items():
            if value is None:
                result.data.attrs[key] = "None"

    def _save_nc4(self, ds: xr.Dataset, sink_path: str, flight_level: int) -> None:
        # Can only save as netcdf3 with file-like objects:
        # https://docs.xarray.dev/en/stable/generated/xarray.Dataset.to_netcdf.html.
        # We want netcdf4, so have to save to a temporary file using its path,
        # then transfer the result to the cloud bucket
        with tempfile.NamedTemporaryFile(delete_on_close=False) as tmp:
            tmp.close()
            # minify netcdf content saved to disk
            ds_attrs = list(ds.attrs.keys())
            for k in ds_attrs:
                # prune dataset-level attributes
                del ds.attrs[k]
            # drop extraneous coords
            req_coords = {"time", "level", "latitude", "longitude"}
            for name, _ in ds.coords.items():
                if name not in req_coords:
                    ds = ds.drop_vars(name)

            # convert vertical coordinates to flight_level
            ds = ds.rename({"level": "flight_level"})
            # assign vertical dimension value to flight level
            # note: this step will also drop any pre-existing attrs that were assigned to 'level'
            ds.coords["flight_level"] = np.array([flight_level]).astype("int16")

            # drop extraneous data vars
            req_data_vars = {"ef_per_m"}
            for name, _ in ds.data_vars.items():
                if name not in req_data_vars:
                    ds = ds.drop_vars(name)

            # cast lat and lon from float64 -> float32
            ds.coords["longitude"] = ds.coords["longitude"].astype("float32")
            ds.coords["latitude"] = ds.coords["latitude"].astype("float32")

            # reorder dimensions and variables (optics change only)
            ds = ds[["longitude", "latitude", "flight_level", "time", "ef_per_m"]]

            ds.to_netcdf(tmp.name, format="NETCDF4")
            fs = gcsfs.GCSFileSystem()
            fs.put(tmp.name, sink_path)
        logger.info(f"netcdf grid written to gcs at: {sink_path}.")

    @staticmethod
    def _build_polygons(
        da_ef_per_m: xr.DataArray, threshold: int
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
            interiors=False,  # schemas.py L1378
            convex_hull=False,  # schemas.py L1417
            include_altitude=True,  # grid.py L601
            lower_bound=True if threshold > 0 else False,
        )
        logger.info(f"building polygon for threshold: {threshold}")
        mda_ef_per_m = MetDataArray(da_ef_per_m)
        poly = mda_ef_per_m.to_polygon_feature(**params)
        return geojson.FeatureCollection([poly])

    @staticmethod
    def _save_geojson(fc: geojson.FeatureCollection, sink_path: str) -> None:
        fs = gcsfs.GCSFileSystem()
        with fs.open(sink_path, "w") as f:
            geojson.dump(fc, f)


@dataclass(frozen=True)
class Message:
    data: bytes
    ack_id: str
    ordering_key: str


class PubSubSubscriptionHandler:
    """
    Handler for managing consumption and marshalling of jobs from a pubsub subscription queue.
    """

    def __init__(
        self,
        subscription: str,
        ack_extension_sec: float = 300,
        pull_timeout_sec: float = 60.0,
    ):
        """
        Parameters
        ----------
        subscription
            The fully-qualified URI for the pubsub subscription.
            e.g. 'projects/contrails-301217/subscriptions/api-preprocessor-sub-dev'
        ack_extension_sec
            Seconds the lease management thread will periodically extend the ack
            deadline for outstanding messages.
        pull_timeout_sec
            Seconds the subscriber client will block for messages before retrying.
        """
        self.subscription = subscription
        self.pull_timeout_sec = pull_timeout_sec
        self.ack_extension_sec = ack_extension_sec

        self._client = pubsub_v1.SubscriberClient()

        self._outstanding_messages: set[Message] = set()

    def _fetch(self) -> Message:
        """Fetch a message from the subscription queue.

        Returns
        -------
        Message
            The dequeued message from the pubsub subscription.

        Raises
        ------
        QueueEmptyError
            No data was returned from the PubSub subscription
        """
        logger.info(f"fetching message from {self.subscription}")
        resp = self._client.pull(
            request={"subscription": self.subscription, "max_messages": 1},
            timeout=self.pull_timeout_sec,
        )

        if len(resp.received_messages) == 0:
            # it is possible there are no messages available,
            # or, pubsub returned zero when there are in fact some messages
            logger.info("zero messages received.")
            raise QueueEmptyError()

        pubsub_msg = resp.received_messages[0]
        logger.info(
            f"received 1 message from {self.subscription}. "
            f"published_time: {pubsub_msg.message.publish_time}, "
            f"message_id: {pubsub_msg.message.message_id}"
        )

        message = Message(
            data=pubsub_msg.message.data,
            ack_id=pubsub_msg.ack_id,
            ordering_key=pubsub_msg.message.ordering_key,
        )
        return message

    def subscribe(self) -> Iterator[Message]:
        """Yields messages from the subscription.

        This method returns an iterator to loop over messages in the subscription. While
        iterating over the result, a sidecar thread will periodically extend the ack
        deadlines associated with outstanding messages to avoid redelivery while work
        is in progress.
        """
        # Start lease manager thread to periodically extend ack deadline.
        exit_when_set = threading.Event()
        lease_manager = threading.Thread(
            target=self._ack_management_worker,
            kwargs=dict(exit_when_set=exit_when_set),
            daemon=True,
        )
        lease_manager.start()

        try:
            while True:
                message = self._fetch()
                self._outstanding_messages.add(message)
                yield message
                # Guard against user failing to call ack() or nack()
                if message in self._outstanding_messages:
                    logger.warning(f"Message was never ack'ed or nack'ed: {message}")
                    self._outstanding_messages.discard(message)
        except GeneratorExit:
            pass

        # Signal lease manager thread exit
        exit_when_set.set()
        # Block until lease manager thread exits
        lease_manager.join()

    def ack(self, message: Message):
        """Acknowledge the message to remove from the queue."""
        # Stop extending lease before server-side ack. This avoids cases where the lease
        # management worker fails to extend the ack deadline for an already ack'ed
        # message, at the cost of a small probability of redelivery.
        try:
            self._outstanding_messages.remove(message)
        except KeyError:
            logger.warning(f"Message ack'ed or nack'ed multiple times: {message}")

        self._client.acknowledge(
            request={"subscription": self.subscription, "ack_ids": [message.ack_id]},
            timeout=30,
        )
        logger.info("successfully ack'ed message.")

    def nack(self, message: Message):
        """Not-acknowledge the message to stop extending ack deadline.

        Does not nack the message server-side, so the message will be retried based on
        the server-side redelivery configuration rather than immediately redelivered to
        another worker.
        """
        try:
            self._outstanding_messages.remove(message)
        except KeyError:
            logger.warning(f"Message ack'ed or nack'ed multiple times: {message}")

    def _ack_management_worker(self, exit_when_set: threading.Event):
        """
        Extends the ack deadline for the currently outstanding message.
        """
        logger.info("starting ack lease management worker...")
        while True:
            should_exit = exit_when_set.wait(self.ack_extension_sec / 2)
            if should_exit:
                break

            # Avoid iterating over a mutable set.
            messages = self._outstanding_messages.copy()
            for message in messages:
                ack_id = message.ack_id
                logger.info(f"extending ack deadline on ack_id: {ack_id[0:-150]}...")
                try:
                    self._client.modify_ack_deadline(
                        request={
                            "subscription": self.subscription,
                            "ack_ids": [ack_id],
                            "ack_deadline_seconds": self.ack_extension_sec,
                        }
                    )
                except Exception:
                    logger.error(
                        "failed to extend ack deadline for message. "
                        f"traceback: {format_traceback()}"
                    )

        logger.info("terminated ack lease management worker")


class PubSubPublishHandler:
    def __init__(self, topic_id: str, ordered_queue: bool) -> None:
        self._topic_id = topic_id

        self._publisher = pubsub_v1.PublisherClient(
            # Batch settings increase payload size to execute fewer, larger requests.
            # See: https://cloud.google.com/pubsub/docs/batch-messaging
            batch_settings=pubsub_v1.types.BatchSettings(
                max_messages=1000,
                max_bytes=20 * 1000 * 1000,  # 20 MB max server-side request size
                max_latency=0.1,  # default: 10 ms
            ),
            publisher_options=pubsub_v1.types.PublisherOptions(
                enable_message_ordering=ordered_queue,
                # Flow control applies rate limits by blocking any time the staged data
                # exceeds the following settings. Once the records are received by GCP
                # PubSub, additional publish calls are unblocked.
                # See: https://cloud.google.com/pubsub/docs/flow-control-messages
                flow_control=pubsub_v1.types.PublishFlowControl(
                    message_limit=100 * 1000,
                    byte_limit=1024 * 1024 * 1024,  # 1 GiB
                    limit_exceeded_behavior=pubsub_v1.types.LimitExceededBehavior.BLOCK,
                ),
                # Retry defaults depend on gRPC method, see default for publish here:
                # https://github.com/googleapis/python-pubsub/blob/ff229a5fdd4deaff0ac97c74f313d04b62720ff7/google/pubsub_v1/services/publisher/transports/base.py#L164-L183
                retry=google.api_core.retry.Retry(
                    initial=0.1,
                    maximum=10,
                    multiplier=2,
                    predicate=google.api_core.retry.if_exception_type(
                        google.api_core.exceptions.Aborted,
                        google.api_core.exceptions.Cancelled,
                        google.api_core.exceptions.DeadlineExceeded,
                        google.api_core.exceptions.InternalServerError,
                        google.api_core.exceptions.ResourceExhausted,
                        google.api_core.exceptions.ServiceUnavailable,
                        google.api_core.exceptions.Unknown,
                    ),
                ),
            ),
        )

        self._publish_futures: list[concurrent.futures.Future] = []

    def publish_async(
        self,
        data: bytes,
        timeout_seconds: float,
        ordering_key: str = "",
        log_context: dict[str, Any] | None = None,
    ) -> None:
        """Add data to the current publish batch.

        Batches are pushed asynchronously to GCP PubSub in a separate thread. To wait
        for one or more publish calls until they have been received by the server, call
        wait_for_publish.

        Parameters
        ----------
        data
            byte encoded string payload
        ordering_key
            payloads sharing the same ordering_key are guaranteed to be delivered to
            consumers in the order they are published. the publisher client,
            and the subscription bound to the receiving topic,
            must be configured to use ordered messages.
        timeout_seconds
            timeout applied to each gRPC call to the PubSub API
        metadata
            any additional k-vs that contextualize the publish event.
            these will be added as context to the publisher callback,
            which includes them in any failure logs.
        """
        future: concurrent.futures.Future = self._publisher.publish(
            topic=self._topic_id,
            data=data,
            ordering_key=ordering_key,
            timeout=timeout_seconds,
        )

        done_callback = self._done_callback_factory(log_context)
        future.add_done_callback(done_callback)
        self._publish_futures.append(future)

    def wait_for_publish(self, timeout_seconds: float | None = None) -> None:
        """Block until all current publish batches are received by server.

        Parameters
        ----------
        timeout_seconds
            Duration to wait for all publish jobs to complete. If timeout_seconds is
            exceeded, the process will be force exited with os._exit(1).
        """
        _, not_done = concurrent.futures.wait(
            self._publish_futures,
            timeout=timeout_seconds,
        )

        # Exit if any publish futures have not completed before configured timeout.
        #
        # We cannot raise an exception or invoke sys.exit from the parent while child
        # threads are still running, because cpython configures a shutdown handler to
        # wait for spawned threads to complete before exiting:
        # https://github.com/python/cpython/blob/8f25cc992021d6ffc62bb110545b97a92f7cb295/Lib/concurrent/futures/thread.py#L18-L37
        #
        # Errors in child threads trigger a separate exit using a future done_callback.
        if not_done:
            logger.error("Futures did not complete before timeout: %s", not_done)
            os._exit(1)

        # All futures completed without error, reset pending futures state.
        self._publish_futures = []

    @staticmethod
    def _done_callback_factory(
        log_context: dict[str, Any] | None,
    ) -> Callable[[concurrent.futures.Future], None]:
        """
        returns a function to use as a callback.
        Constructs a log message annotating with any k-vs passed to this method.
        """
        msg = ""
        if log_context:
            for k, v in log_context.items():
                msg += f" {k}={v} "

        def _exit_on_error(future: concurrent.futures.Future) -> None:
            """Re-raise any exceptions raised by the future's execution thread.

            This should be registered as a callback that will only be invoked when the future
            has already completed using:
                future.add_done_callback(_raise_exception_if_failed)
            """
            try:
                future.result(timeout=0)
            except Exception:
                logger.error(
                    f"Publish future failed: {msg}. Unhandled exception:"
                    + format_traceback()
                )
                os._exit(1)

        return _exit_on_error


class ZarrRemoteFileHandler:
    """
    Handler for managing localization of remote GCS dirs/files.
    """

    TMPDIR = "/tmp"
    DOWNLOAD_THREAD_CNT = 100

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
