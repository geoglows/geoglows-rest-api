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
RUN apt-get update && apt-get install -y --no-install-recommends curl vim && rm -rf /var/lib/apt/lists/*
RUN chmod +x /startup.sh
RUN micromamba create -n app-env --yes --file "environment.yaml" && micromamba clean --all --yes

ARG MAMBA_DOCKERFILE_ACTIVATE=1

EXPOSE 80

HEALTHCHECK --interval=1m --timeout=3s --start-period=20s \
  CMD curl -f http://localhost/ || exit 1

ENTRYPOINT [ "/startup.sh" ]
