FROM debian:stable

MAINTAINER Azavea

RUN apt-get -y update && \
    apt-get -y install gdal-bin python libgdal-dev python-dev python-pip python-numpy && \
    pip install rasterio==0.31.0 boto==2.39.0 && \
    apt-get -y purge libgdal-dev python-dev && \
    apt-get -y autoremove

COPY chunker/ /chunker
WORKDIR /chunker
