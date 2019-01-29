FROM ubuntu:16.04

ENV LANG=C.UTF-8 LC_ALL=C.UTF-8
ENV PATH /opt/conda/bin:$PATH
RUN mkdir /var/uwsgi

RUN apt-get update --fix-missing && \
    apt-get install -y wget supervisor bzip2 && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN apt-get -qq update && apt-get -qq -y install curl bzip2 \ 
    && curl -sSL https://repo.continuum.io/miniconda/Miniconda3-4.5.4-Linux-x86_64.sh -o /tmp/miniconda.sh \
    && bash /tmp/miniconda.sh -bfp /usr/local \ 
    && rm -rf /tmp/miniconda.sh \ 
    && conda install -y python=3 \ 
    && conda update conda \ 
    && apt-get -qq -y remove curl bzip2 \ 
    && apt-get -qq -y autoremove \ 
    && apt-get autoclean \ 
    && rm -rf /var/lib/apt/lists/* /var/log/dpkg.log \ 
    && conda clean --all --yes

RUN conda create -n gsp_api python=3.6.6 \
    && echo "source activate gsp_api" >> ~/.bashrc \
    && conda install -c conda-forge -n gsp_api uwsgi flask flask-restful

RUN apt-get update
RUN apt-get install apt-transport-https -y

RUN apt-get update && apt-get install -y --no-install-recommends \
		ca-certificates \
		curl \
    && rm -rf /var/lib/apt/lists/*

ENV PATH /usr/local/envs/gsp_api/bin:$PATH

RUN apt-get update
RUN apt-get -y install sudo

RUN echo "deb [arch=amd64] https://packages.microsoft.com/repos/microsoft-ubuntu-xenial-prod/ xenial main" > azure.list
RUN sudo cp ./azure.list /etc/apt/sources.list.d/
RUN sudo apt-key adv --keyserver packages.microsoft.com --recv-keys EB3E94ADBE1229CF
RUN apt-get update
RUN apt-get install -y azcopy

RUN echo "source activate gsp_api" >> ~/.bashrc \
    && conda install -c conda-forge -n gsp_api numpy pandas xarray netCDF4

RUN sudo apt-get install vim -y

# Copy API code
COPY ./GSP_API /app/GSP_API/
COPY ./supervisord.conf /etc/supervisord.conf

# startup.sh is a helper script
COPY ./startup.sh /
RUN chmod +x /startup.sh

# Copy file connection information
COPY ./file_mount.json /app/azcopy/file_mount.json

COPY ./file_mounter.py /app/azcopy/file_mounter.py
RUN chmod +x /app/azcopy/file_mounter.py

ENV API_PREFIX=/v1/global_streamflow_prediction/get_streamflow

# Expose the port that is to be used when calling your API
EXPOSE 80
HEALTHCHECK --interval=1m --timeout=3s --start-period=20s \
  CMD curl -f http://localhost/ || exit 1
ENTRYPOINT [ "/startup.sh" ]
