FROM python:3.9-slim
LABEL maintainer="edenceHealth NV <info@edence.health>"

ARG AG="env DEBIAN_FRONTEND=noninteractive apt-get -yq --no-install-recommends"
RUN set -eux; \
  $AG update; \
  $AG install --no-install-recommends \
    gcc \
    python3-dev \
  ; \
  $AG autoremove; \
  $AG clean; \
  rm -rf \
    /var/lib/apt/lists/* \
    /var/lib/dpkg/*-old /var/cache/debconf/*-old \
    /tmp/* \
  ;

ARG DEV_BUILD=""
ENV DEV_BUILD ${DEV_BUILD}

COPY requirements.txt requirements.dev.txt /
RUN set -eux; \
  pip install --upgrade pip; \
  pip install -r /requirements.txt; \
  if [ -n "$DEV_BUILD" ]; then pip install -r /requirements.dev.txt; fi; \
  pip cache purge;

# these 2 variables are expected by the app code
ARG COMMIT_SHA
ARG GITHUB_TAG
ENV COMMIT_SHA="${COMMIT_SHA}" GITHUB_TAG="${GITHUB_TAG}"
RUN set -eu; \
  : "${COMMIT_SHA:?the COMMIT_SHA build-arg cannot be empty: read docs}"; \
  : "${GITHUB_TAG:?the GITHUB_TAG build-arg cannot be empty: read docs}";

WORKDIR /etl
COPY . .

ENTRYPOINT ["./docker/entrypoint.sh"]
