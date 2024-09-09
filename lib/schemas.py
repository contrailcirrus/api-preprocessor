""" Data Object Models & Schemas"""

import hashlib
from dataclasses import dataclass, asdict
import json


@dataclass
class ApiPreprocessorJob:
    """
    Defines the data model for messages published to the pubsub topic for the API Preprocessor.

    Fields:
      - model_run_at: unixtime at which model was executed
      - model_predicted_at: unixtime at which model predicts outputs quantities
      - flight_level: the flight level for this job's unit of work
      - aircraft_class: the string literal defining the aircraft class

    """

    # predefined flight levels
    FLIGHT_LEVELS = [
        270,
        280,
        290,
        300,
        310,
        320,
        330,
        340,
        350,
        360,
        370,
        380,
        390,
        400,
        410,
        420,
        430,
        440,
    ]

    # predefined aircraft classes
    AIRCRAFT_CLASSES = {
        "low_e": {
            "aircraft_type_icao": "B789",
            "engine_uid": "01P17GE211",
        },
        "default": {
            "aircraft_type_icao": "B738",
            "engine_uid": "01P11CM116",
        },
        "high_e": {
            "aircraft_type_icao": "A320",
            "engine_uid": "01P10IA021",
        },
    }

    model_run_at: int
    model_predicted_at: int
    flight_level: int
    aircraft_class: str

    def as_utf8_json(self) -> bytes:
        """
        Builds a utf-8 encoded JSON blob from the class' attributes.
        """
        js = json.dumps(asdict(self))
        return js.encode("utf-8")

    @staticmethod
    def from_utf8_json(blob: bytes):
        """
        Takes a utf8 json blob and marshals to an instance of ApiPreprocessorJob.

        Parameters
        ----------
        blob
            json blob with required ApiPreprocessorJob fields, in utf8
            e.g. b'{"model_run_at": 1709143200, "model_predicted_at": 1709150400, "flight_level": 290, "aircraft_class": "default"}'
        """
        return ApiPreprocessorJob(**json.loads(blob))


@dataclass
class RegionsBigQuery:
    """
    A CoCip regions (aka polygon) object, formatted for export to BigQuery.

    The GCP pubsub-to-bigquery adapter does not support writing native BQ Geography objects.
    As such, we write stringified geojson to BQ, with the expectation of using the
    st_geogfromgeojson method for on-the-fly conversion.

    Refs:
    - https://cloud.google.com/bigquery/docs/reference/standard-sql/geography_functions#st_geogfromgeojson
    ```
    The input JSON fragment must consist of a GeoJSON geometry type,
    which includes Point, MultiPoint, LineString, MultiLineString, Polygon,
    MultiPolygon, and GeometryCollection.
    Any other GeoJSON type such as Feature or FeatureCollection will result in an error.
    ```
    """

    aircraft_class: str
    flight_level: int
    timestamp: int  # unixtime in seconds; predicted_at time for cocip model run
    hres_model_run_at: int  # unixtime in seconds; model_run_at time for the hres data
    threshold: int  # threshold value for the generated polygons
    regions: str  # string geojson geometry (MultiPolygon) object

    def to_bq_flatmap(self) -> bytes:
        """
        Returns a single utf-8 encoded json string literals,
        ready for egress to big query.

        Converts temporal fields to microseconds epoch.

        Adds an `_instance_hash` k-v, of type int,
        generated as a hash of the composite <flight_level><timestamp><hres_model_run_at><regions>,
        where timestamp is epoch time in microseconds
        """
        base = asdict(self)
        base["timestamp"] = base["timestamp"] * 1e6
        base["hres_model_run_at"] = base["hres_model_run_at"] * 1e6
        hash = hashlib.md5(
            f"{base['flight_level']}{base['timestamp']}"
            f"{base['hres_model_run_at']}{base['regions']}".encode("utf-8")
        )
        # truncate as to be equal or smaller than int64 space when represented as signed int
        hash_trunc = hash.hexdigest()[:8]
        hash_int = int(hash_trunc, 16)
        base["_instance_hash"] = hash_int
        return json.dumps(base).encode("utf-8")
