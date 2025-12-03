FROM jupyter/datascience-notebook:latest

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER ${NB_UID}

COPY notebooks/requirements.txt /tmp/requirements.txt

RUN pip install --no-cache-dir -r /tmp/requirements.txt

WORKDIR /home/jovyan/work

EXPOSE 8888
