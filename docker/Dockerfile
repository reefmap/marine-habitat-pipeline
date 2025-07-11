###############################################################################
#  Dockerfile — Marine-Habitat-Pipeline (+ ACOLITE)
###############################################################################

# ---------- 1. Base image ----------
FROM python:3.10-slim

# ---------- 2. System libraries ----------
RUN apt-get update && DEBIAN_FRONTEND=noninteractive \
    apt-get install -y --no-install-recommends \
        gdal-bin libgdal-dev python3-gdal \   
        libproj-dev proj-bin proj-data \
        libgeos-dev libspatialindex-dev \
        build-essential \
        wget curl ca-certificates \
        unzip \
        git \
        openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*

ENV CPLUS_INCLUDE_PATH=/usr/include/gdal
ENV C_INCLUDE_PATH=/usr/include/gdal

# 3.----------  Install ACOLITE (Python/Java CLI via GitHub)---------- 
RUN git clone https://github.com/acolite/acolite.git /opt/acolite \
 && ln -sf /opt/acolite/acolite/acolite/acolite_run.py /opt/acolite/acolite.py

# Expose a single “friendly” entrypoint and also record the true path  
ENV ACOLITE_CLI=/opt/acolite/acolite/acolite_run.py  
ENV ACOLITE_HOME=/opt/acolite  
ENV PATH="${ACOLITE_HOME}:${PATH}"

# ---------- 4. Python packages ----------
RUN pip install --no-cache-dir -U pip \
 && pip install --no-cache-dir "GDAL==$(gdal-config --version)"  

RUN pip install --no-cache-dir \
        geopandas==0.14.* rasterio==1.3.* rioxarray shapely pyproj fiona rtree \
        scikit-learn xarray netCDF4 sentinelsat cdsapi \
        google-cloud-storage earthengine-api geemap \
        trimesh pyvista \
        jupyterlab==4.*

# ---------- 5. Copy project code ----------
WORKDIR /workflow
COPY . /workflow

# ---------- 6. Earth-Engine CLI check ----------
RUN earthengine --version

# ---------- 7. Docs for proxy + auth (optional, unchanged) ----------
# ARG HTTP_PROXY
# ARG HTTPS_PROXY
# ENV HTTP_PROXY=${HTTP_PROXY}
# ENV HTTPS_PROXY=${HTTPS_PROXY}

# ---------- 8. Default Jupyter entrypoint ----------
EXPOSE 8888
ENV PYTHONPATH=/workflow
ENV JUPYTER_TOKEN=reefmap
CMD ["jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root"]

###############################################################################
#  Build (Intel / AMD)             :  docker build -t reefmap/mhp .
#  Build (Apple M-series)          :  docker build --platform=linux/amd64 -t reefmap/mhp .
###############################################################################
