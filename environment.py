"""
A singleton object intended as the single point of access for application environment variables.

The application may expect required and optional environment variables.
Required environment variables should be imported first, followed by optional environment variables.
"""

import os

SINK_PATH = os.environ["SINK_PATH"]
API_PREPROCESSOR_SUBSCRIPTION_ID = os.environ["API_PREPROCESSOR_SUBSCRIPTION_ID"]

LOG_LEVEL = os.environ.get("LOG_LEVEL", "DEBUG")
