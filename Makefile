REMOTE_ZARR_PATH = "gs://contrails-301217-sandbox-internal/prod"
LOCAL_ZARR_PATH = "./data/prod"

LOG_LEVEL = "DEBUG"
SINK_PATH = "gs://contrails-301217-api-preprocessor-dev"
API_PREPROCESSOR_SUBSCRIPTION_ID = "foobar"

benchmark_remote:
	export LOG_LEVEL=$(LOG_LEVEL) && \
	export SOURCE_PATH=$(REMOTE_ZARR_PATH) && \
	export SINK_PATH=$(SINK_PATH) && \
	export API_PREPROCESSOR_SUBSCRIPTION_ID=$(API_PREPROCESSOR_SUBSCRIPTION_ID) && \
	mprof run --output output/remote.mprof python3 main.py > output/remote.stdout 2> output/remote.stderr

report_remote:
	mprof peak output/remote.mprof
	python3 analyze_stdout.py output/remote.stdout
	python3 analyze_stderr.py output/remote.stderr

benchmark_local:
	export LOG_LEVEL=$(LOG_LEVEL) && \
	export SOURCE_PATH=$(LOCAL_ZARR_PATH) && \
	export SINK_PATH=$(SINK_PATH) && \
	export API_PREPROCESSOR_SUBSCRIPTION_ID=$(API_PREPROCESSOR_SUBSCRIPTION_ID) && \
	mprof run --output output/local.mprof python3 main.py > output/local.stdout 2> output/local.stderr

report_local:
	mprof peak output/local.mprof
	python3 analyze_stdout.py output/local.stdout
	python3 analyze_stderr.py output/local.stderr

benchmark_remote_no_dask:
	export LOG_LEVEL=$(LOG_LEVEL) && \
	export SOURCE_PATH=$(REMOTE_ZARR_PATH) && \
	export SINK_PATH=$(SINK_PATH) && \
	export API_PREPROCESSOR_SUBSCRIPTION_ID=$(API_PREPROCESSOR_SUBSCRIPTION_ID) && \
	export NO_DASK=1 && \
	mprof run --output output/remote_no_dask.mprof python3 main.py > output/remote_no_dask.stdout 2> output/remote_no_dask.stderr

report_remote_no_dask:
	mprof peak output/remote_no_dask.mprof
	python3 analyze_stdout.py output/remote_no_dask.stdout
	python3 analyze_stderr.py output/remote_no_dask.stderr
