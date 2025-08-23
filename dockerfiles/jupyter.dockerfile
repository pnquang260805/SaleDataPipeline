FROM jupyter/pyspark-notebook:x86_64-ubuntu-22.04

COPY ./requirements.txt .

USER root

RUN pip install -r ./requirements.txt

USER jovyan