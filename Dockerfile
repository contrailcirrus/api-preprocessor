ARG PYTHON_VERSION=3.12.2

FROM python:${PYTHON_VERSION} AS venv

# Tell pipenv to create venv in the current directory
ENV PIPENV_VENV_IN_PROJECT=1
# Allow statements and log messages to immediately appear in the Knative logs
ENV PYTHONUNBUFFERED 1

COPY Pipfile.lock /
WORKDIR /

# install build tools and compute crcmod/crc32c
# used by service to download remote gs:// zarr store to k8s pod storage
RUN apt-get update -yq && apt-get install -y build-essential

# install production dependencies in pipenv-controlled virtual env
RUN pip install -U pip
RUN pip install pipenv
RUN pipenv sync

# graphics libs for running cocip polygon gen
# RUN apt-get update && apt-get install -y libgl1

# Copy the application code
COPY lib lib
COPY main.py .

ENV PATH /.venv/bin:$PATH
ENV VERSION v0.0.0-dev.0


CMD ["python3", "main.py"]
