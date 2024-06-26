#!/usr/bin/env bash
set -e

JOBS_FILEPATH="ecmwf_workflow_jobs_2024-06-25T14:32:29.log"

echo "checking that files exist for jobs in ${JOBS_FILEPATH}..."
while read p; do
  echo ${p}
  LST="$(gsutil ls "gs://contrails-301217-ecmwf-hres-delivery-be/${p}")"
  echo ${LST}
done <${JOBS_FILEPATH}

echo "submitting list of jobs to GCP Workflow..."
# cat ${JOBS_FILEPATH} | xargs -I % sh -c 'gcloud workflows execute event-arc-triggered-workflow-prod --location=us-east1 --data="{\"data\":{\"bucket\":\"contrails-301217-ecmwf-hres-delivery-be\", \"name\":\"%\"}}"; sleep 25'

