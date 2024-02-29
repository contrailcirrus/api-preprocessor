# Set python version
# https://hub.docker.com/_/python
ARG PYTHON_VERSION=3.12.2

# -----------------------------
# Stage 1: Install dependencies
# -----------------------------
FROM python:${PYTHON_VERSION} AS venv

# Tell pipenv to create venv in the current directory
ENV PIPENV_VENV_IN_PROJECT=1

COPY Pipfile.lock /usr/src/

WORKDIR /usr/src

# install production dependencies in pipenv-controlled virtual env
RUN pip install -U pip
RUN pip install pipenv
RUN pipenv sync

# ---------------------------------
# Stage 2: Build a production image
# ---------------------------------
# Use the official slim Python image.
FROM python:${PYTHON_VERSION}-slim AS prod

# Allow statements and log messages to immediately appear in the Knative logs
ENV PYTHONUNBUFFERED 1

# Copy the virtual environment from the previous stage
RUN mkdir -v /usr/src/.venv
COPY --from=venv /usr/src/.venv/ /usr/src/.venv/
ENV PATH /usr/src/.venv/bin:$PATH

# Copy the application code
WORKDIR /app
COPY app app

ENV VERSION v0.0.0-dev.0


CMD exec hypercorn app.main:app --bind :$PORT
