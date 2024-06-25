#!/usr/bin/env bash

JOBS_FILEPATH="ecmwf_workflow_jobs_2024-06-25T14:29:41.log"

echo "checking that files exist for jobs in ${JOBS_FILEPATH}..."
cat ${JOBS_FILEPATH} | xargs -I % sh -c 'gcloud workflows execute event-arc-triggered-workflow-prod --location=us-east1 --data="{\"data\":{\"bucket\":\"contrails-301217-ecmwf-hres-delivery-be\", \"name\":\"%\"}}"; sleep 25'

