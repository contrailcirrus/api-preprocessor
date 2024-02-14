""" Data Object Models & Schemas"""

from dataclasses import dataclass
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
    """

    # predefined flight levels for API preprocessor jobs
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

    model_run_at: int
    model_predicted_at: int
    flight_level: Literal[*FLIGHT_LEVELS]

    def as_utf8_json(self) -> bytes:
        """
        Builds a utf-8 encoded JSON blob from the class' attributes.
        """
        js = json.dumps(self.__dict__)
        return js.encode("utf-8")
