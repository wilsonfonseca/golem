# Dockerfile for a base image for computing tasks in Golem.
# Installs python and sets up directories for Golem tasks.

FROM golang:1.12.7 as stats-builder
RUN git clone --depth 1 --branch 0.1 https://github.com/golemfactory/docker-cgroups-stats.git /build
WORKDIR /build
RUN go build -o docker-cgroups-stats main.go

FROM nvidia/cudagl:9.2-runtime-ubuntu18.04

MAINTAINER Golem Tech <tech@golem.network>

COPY --from=stats-builder /build/docker-cgroups-stats /usr/bin

RUN set -x \
    && apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates wget curl \
    && apt-get install -y python3.6 \
    && apt-get clean \
    && apt-get -y autoremove \
    && rm -rf /var/lib/apt/lists/* \
    && ln -s /usr/bin/python3.6 /usr/bin/python3

RUN mkdir /golem \
 && mkdir /golem/work \
 && mkdir /golem/resources \
 && mkdir /golem/output

COPY core/resources/images/scripts/ /golem/
RUN chmod +x /golem/install_py_libs.sh

WORKDIR /golem/work/

ENV DISPLAY=""
ENV LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/cuda/lib:/usr/local/cuda/lib64
