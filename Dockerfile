FROM python:3.12-slim
LABEL maintainer="Intensive Care Department 4131 - Rigshospitalet"
LABEL org.opencontainers.image.source="https://github.com/INCEPTdk/omop_etl"

ARG AG="env DEBIAN_FRONTEND=noninteractive apt-get -yq --no-install-recommends"
RUN set -eux; \
  $AG update; \
  $AG install \
    build-essential \
    wget \
    unzip \
  ; \
  $AG autoremove; \
  $AG clean; \
  rm -rf \
    /var/lib/apt/lists/* \
    /var/lib/dpkg/*-old /var/cache/debconf/*-old \
    /tmp/* \
  ;

ARG DEV_BUILD=""
ENV DEV_BUILD=${DEV_BUILD}

COPY requirements.txt requirements.dev.txt /
RUN set -eux; \
  pip install --upgrade pip; \
  pip install -r /requirements.txt; \
  if [ -n "$DEV_BUILD" ]; then pip install -r /requirements.dev.txt; fi; \
  pip cache purge;

RUN wget https://github.com/duckdb/duckdb/releases/download/v1.0.0/duckdb_cli-linux-amd64.zip && \
    unzip duckdb_cli-linux-amd64.zip && \
    mv duckdb /usr/local/bin/ && \
    chmod +x /usr/local/bin/duckdb && \
    rm duckdb_cli-linux-amd64.zip

# these 2 variables are expected by the app code
ARG COMMIT_SHA="dev"
ARG GITHUB_TAG="dev"
ENV COMMIT_SHA="${COMMIT_SHA}" GITHUB_TAG="${GITHUB_TAG}"

WORKDIR /etl
COPY . .

RUN mkdir /data
RUN mkdir /vocab
RUN mkdir /users

ENTRYPOINT ["./docker/entrypoint.sh"]
