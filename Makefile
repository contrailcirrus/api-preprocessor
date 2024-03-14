
SOURCE_PATH_COARSE = "gs://contrails-301217-sandbox-internal/coarse"
SOURCE_PATH_FINE = "gs://contrails-301217-sandbox-internal/fine"

LOG_LEVEL = "INFO"
SINK_PATH = "gs://contrails-301217-api-preprocessor-dev"
API_PREPROCESSOR_SUBSCRIPTION_ID = "foobar"

run_coarse:
	export LOG_LEVEL=$(LOG_LEVEL) && \
	export SOURCE_PATH=$(SOURCE_PATH_COARSE) && \
	export SINK_PATH=$(SINK_PATH) && \
	export API_PREPROCESSOR_SUBSCRIPTION_ID=$(API_PREPROCESSOR_SUBSCRIPTION_ID) && \
	python3 main.py