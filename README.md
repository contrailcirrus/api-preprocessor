# api-preprocessor

## Overview
The API Preprocessor runs the pycontrails CoCip grid model on HRES meteorological forecasts,
and persists those assets to Google Cloud Storage (GCS) and BigQuery (BQ).
The static assets written to GCS back the [`v1` routes](https://api.contrails.org/openapi#/Production%20(beta)) 
of the [pycontrails API](https://apidocs.contrails.org/#production-api-beta).

## Behavior
The API Preprocessor is a Dockerized python application designed to be a worker service
which consumes jobs from a job queue.

A "job" is an `ApiPreprocessorJob`, and defined in [`lib/schemas.py](lib/schemas.py).
A job fully defines the work to be done by a single invocation of the API Preprocessor:
```text
- model_run_at: unixtime at which model was executed
- model_predicted_at: unixtime at which model predicts outputs quantities
- flight_level: the flight level for this job's unit of work
- aircraft_class: the string literal defining the aircraft class
```

`model_run_at` defines the target HRES data source to use for running CoCip.
Every six hours, ECMWF runs the HRES forecast model. The time at which this model is run
is the `model_run_at` time.

`model_predicted_at` defines the target timestamp for running CoCip .
It is expected that `model_predicted_at` occurs on or after `model_run_at`,
occurring no greater than 72 hours past `model_run_at` 
(ECMWF provides meteorological forecasts 72 hours past `model_predicted_at` for a given model run).

The acceptable elapsed time between `model_run_at` and `model_predicted_at` is further constrained
by a maximum assumed age for contrail evolution. 
For reference, see `ValidationHandler.sufficient_forecast_range()` in [lib/handlers.py](lib/handlers.py).

`flight_level` defines the flight level on which to calculate the CoCip grid.
The API Preprocessor may also be directed to run across all flight levels,
if the `ApiPreprocessor.flight_level` parameter is set to the 
`ApiPreprocessor.ALL_FLIGHTS_LEVEL_WILDCARD`, then the API Preprocessor will
run CoCip across all flight levels.  The determination to run the API Preprocessor
on a single fl or across all fls depends on how the implementer wishes to manage
concurrency when implementing the API Preprocessor in-system.
Note that the resource configuration (memory, vCPU) defined in the [service's k8s yaml](helm/templates/api-preprocessor-cronjob.yaml)
are currently set assuming the API Preprocessor is running across all flight levels 
(i.e, it is currently set to higher memory and vCPU settings than if it were running on single fls).

`aircraft_class` defines the friendly-name identifier for the aircraft configuration to use
when running CoCip (passed to the aircraft performance model).
At present, three aircraft classes are defined, each mapping to an aircraft body type and engine type.
See `ApiPreprocessorJob.AIRCRAFT_CLASSES` in [schemas.py](lib/schemas.py).


## Environment Variables
The following environment variables are expected for production and development environments.

| name                              |                                                description                                                |
|:----------------------------------|:---------------------------------------------------------------------------------------------------------:|
| SOURCE_PATH                       |                       fully-qualified file path for HRES pressure-level zarr stores                       |
| SINK_PATH                         |                      fully-qualified file path prefix for writing output data assets                      |
| API_PREPROCESSOR_SUBSCRIPTION_ID  | fully-qualified uri for the pubsub subscription for api-preprocessor jobs (generated by the hres-etl svc) |
| COCIP_REGIONS_BQ_TOPIC_ID         |        fully-qualified uri for the pubsub topic that injects records to the bigquery regions table        |
| API_PREPROCESSOR_SUBSCRIPTION_ACK_EXTENSION_SEC                         |                         extension period for subscriber message lease management                          |
| LOG_LEVEL                         |                                    log level for service in production                                    |


## Deploy
Merges to the `develop` branch build and deploy this service to the dev environment,
consuming jobs from a pubsub subscription and writing output assets to a gcs path/pubsub topic as defined
in the environment variables of [`deploy-dev.yaml`](.github/workflows/deploy-dev.yaml).

Merges to the `main` branch similarly deploy this service to the prod environment.
See [`deploy-main.yaml](.github/workflows/deploy-prod.yaml).