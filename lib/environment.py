"""
A singleton object intended as the single point of access for application environment variables.

The application may expect required and optional environment variables.
Required environment variables should be imported first, followed by optional environment variables.
"""

import os

SOURCE_PATH = os.environ["SOURCE_PATH"]
SINK_PATH = os.environ["SINK_PATH"]
API_PREPROCESSOR_SUBSCRIPTION_ID = os.environ["API_PREPROCESSOR_SUBSCRIPTION_ID"]
COCIP_REGIONS_BQ_TOPIC_ID = os.environ["COCIP_REGIONS_BQ_TOPIC_ID"]

API_PREPROCESSOR_SUBSCRIPTION_ACK_EXTENSION_SEC = int(
    os.environ.get("API_PREPROCESSOR_SUBSCRIPTION_ACK_EXTENSION_SEC", 300)
)
LOG_LEVEL = os.environ.get("LOG_LEVEL", "DEBUG")
