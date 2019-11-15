FROM continuumio/miniconda3:latest

ENV LANG=C.UTF-8 LC_ALL=C.UTF-8 PATH /opt/conda/envs/gsp_api/bin:$PATH API_PREFIX=/api
RUN mkdir /var/uwsgi

RUN apt-get update --fix-missing && \
    apt-get install -y wget tar supervisor bzip2 curl apt-transport-https ca-certificates curl vim cron && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

COPY ./environment.yml ./startup.sh ./

RUN conda config --set channel_priority strict && \
    conda config --add channels conda-forge && \
    conda env create -f environment.yml && \
    echo "conda activate gsp_api" >> ~/.bashrc

RUN mkdir -p /mnt/output/ecmwf && \
    mkdir -p /mnt/output/era

# Copy API code
COPY ./GSP_API /app/GSP_API/
COPY ./supervisord.conf /etc/supervisor/conf.d/uwsgi.conf

# startup.sh is a helper script
RUN chmod +x /startup.sh
    
# Copy files
COPY ./azcopy /app/
RUN chmod -R +x /app/azcopy/

# Expose the port that is to be used when calling your API
EXPOSE 80
HEALTHCHECK --interval=1m --timeout=3s --start-period=20s \
  CMD curl -f http://localhost/ || exit 1
ENTRYPOINT [ "/startup.sh" ]