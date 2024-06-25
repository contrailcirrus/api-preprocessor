"""
Some scripting to generate a list of target google Workflow jobs, to be submitted to the ecmwf -> zarr service.

Given a target model_run_at date, this generates a list of all the expected grib filenames that
that should have been delivered to our cloud bucket.

Those can be submitted individually or in aggregate to the GCP Worklow instance using the
gcloud command, as in the namesake bash script.

This scripting may/should evolve into formal tooling for managing manual re-processing of
ecmwf grib to zarr jobs (for use in failure remediation etc).
"""

from datetime import datetime, UTC
import pendulum
import pandas as pd

MODEL_RUN_AT = 1719036000
OUTPUT_FILE = (
    f'tools/ecmwf_workflow_jobs_{datetime.now().strftime("%Y-%m-%dT%H:%M:%S")}.log'
)


name_fmt = "T1S{model_run_at}00{model_predicted_at}001"
ecmwf_fnames = []
model_run_at_dt = datetime.fromtimestamp(MODEL_RUN_AT, UTC)

interval = pendulum.interval(
    model_run_at_dt,
    model_run_at_dt + pd.to_timedelta(72, "hour"),
)

predicted_at = [dt.strftime("%m%d%H") for dt in interval.range("hours")]
for dt in predicted_at:
    ecmwf_fn = name_fmt.format(
        model_run_at=model_run_at_dt.strftime("%m%d%H"),
        model_predicted_at=dt,
    )
    ecmwf_fnames.append(ecmwf_fn)

with open(OUTPUT_FILE, "w") as fp:
    for ln in ecmwf_fnames:
        fp.write(f"{ln}\n")
