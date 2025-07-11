###############################################################################
#  Dockerfile — Marine-Habitat-Pipeline (ACOLITE included, reqs.txt driven)
###############################################################################

########### 1 ▸ Base image ####################################################
FROM python:3.10-slim

ARG DEBIAN_FRONTEND=noninteractive

########### 2 ▸ OS-level libs #################################################
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        gdal-bin libgdal-dev python3-gdal \
        libproj-dev proj-bin proj-data \
        libgeos-dev libspatialindex-dev \
        build-essential git wget curl unzip ca-certificates \
        openjdk-17-jre-headless && \
    rm -rf /var/lib/apt/lists/*

ENV CPLUS_INCLUDE_PATH=/usr/include/gdal
ENV C_INCLUDE_PATH=/usr/include/gdal

########### 3 ▸ ACOLITE #######################################################
# Shallow-clone so the image stays lean (≈80 MB instead of 300 MB)
RUN git clone --depth 1 https://github.com/acolite/acolite.git /opt/acolite

# Keep original path and ALSO expose it on $PATH
ENV ACOLITE_HOME=/opt/acolite
ENV PATH="$ACOLITE_HOME:$PATH"

########### 4 ▸ Python wheels #################################################
RUN pip install --no-cache-dir -U pip && \
    pip install --no-cache-dir "GDAL==$(gdal-config --version)"

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

########### 5 ▸ Copy project code ############################################
WORKDIR /workflow
COPY . /workflow
ENV PYTHONPATH=/workflow

########### 6 ▸ JupyterLab default entrypoint ################################
 EXPOSE 8888
 ENV JUPYTER_TOKEN=reefmap
 CMD ["jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root"]

###############################################################################
# Build from repo root:
#   docker build -f docker/Dockerfile -t ghcr.io/reefmap/mhp:latest .
###############################################################################
