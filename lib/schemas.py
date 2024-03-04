""" Data Object Models & Schemas"""

from dataclasses import dataclass, asdict
import json
from typing import Literal


@dataclass
class ApiPreprocessorJob:
    """
    Defines the data model for messages published to the pubsub topic for the API Preprocessor.
    Consumers of these jobs include:
      - api-preprocessor worker (generates gridded data output and regions data output)

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
    AIRCRAFT_CLASSES = [
        "default",
    ]

    model_run_at: int
    model_predicted_at: int
    flight_level: Literal[*FLIGHT_LEVELS]
    aircraft_class: Literal[*AIRCRAFT_CLASSES]

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
