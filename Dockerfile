FROM mambaorg/micromamba

USER root

ENV LANG=C.UTF-8 LC_ALL=C.UTF-8
ENV PATH=/opt/conda/envs/app-env/bin:$PATH
ENV API_PREFIX=/api
ENV PYTHONPATH=$PYTHONPATH:/app

COPY --chown=$MAMBA_USER:$MAMBA_USER environment.yaml /environment.yaml
COPY startup.sh /startup.sh
COPY app /app

WORKDIR /

RUN mkdir -p /var/log/uwsgi
RUN apt-get update && apt-get install -y --no-install-recommends curl vim awscli && rm -rf /var/lib/apt/lists/*

# startup.sh is a helper script
RUN chmod +x /startup.sh
RUN micromamba create -n app-env --yes --file "environment.yaml" && micromamba clean --all --yes

# download a copy of the package metadata table
RUN wget http://geoglows-v2.s3-us-west-2.amazonaws.com/tables/package-metadata-table.parquet -O /app/package-metadata-table.parquet
ENV PYGEOGLOWS_METADATA_TABLE_PATH=/app/package-metadata-table.parquet

# download the return periods zarr to avoid network errors
RUN aws s3 cp s3://geoglows-v2-retrospective/return-periods.zarr /app/return-periods.zarr --recursive --no-sign-request

ARG MAMBA_DOCKERFILE_ACTIVATE=1
ENV AWS_LOG_GROUP_NAME=geoglows.ecmwf.int
ENV AWS_LOG_STREAM_NAME=rest_api_metrics
ENV AWS_REGION=eu-central-1

# Expose the port that is to be used when calling your API
EXPOSE 80

HEALTHCHECK --interval=1m --timeout=3s --start-period=20s \
  CMD curl -f http://localhost/ || exit 1

ENTRYPOINT [ "/startup.sh" ]
