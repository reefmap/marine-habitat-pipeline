###############################################################################
#  Dockerfile — Marine-Habitat-Pipeline
#
#  Works on Windows, macOS (Intel / Apple Silicon) and Linux.
#  Each major step is heavily commented so new users understand:
#   • how to build for ARM chips (M1/M2)          →  --platform notes
#   • how to pass proxy variables                 →  HTTP_PROXY / HTTPS_PROXY
#   • how to mount Earth-Engine & GCS credentials →  docker run –v / -e examples
#
#  Build  (x86 desktops) :  docker build -t reefmap/mhp .
#  Build  (Apple Silicon):  docker build --platform=linux/amd64 -t reefmap/mhp .
#  Run                   :  docker run -it -p 8888:8888 -v "$PWD:/workflow" reefmap/mhp
###############################################################################

# ---------- 1. Base image ----------
# python:3.10-slim is x86-64.  Apple-Silicon users should add:
#   --platform=linux/amd64   at build AND run time.
FROM python:3.10-slim

# ---------- 2. System libraries ----------
# GDAL, PROJ, GEOS for rasterio & geopandas; build-essential for wheels.
RUN apt-get update && DEBIAN_FRONTEND=noninteractive \
    apt-get install -y --no-install-recommends \
        gdal-bin libgdal-dev \
        libproj-dev proj-bin proj-data \
        libgeos-dev libspatialindex-dev \
        build-essential \
        wget curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Point rasterio to GDAL headers
ENV CPLUS_INCLUDE_PATH=/usr/include/gdal
ENV C_INCLUDE_PATH=/usr/include/gdal

# ---------- 3. Python packages ----------
RUN pip install --no-cache-dir -U pip

# Geo stack + cloud + notebooks
RUN pip install --no-cache-dir \
        geopandas==0.14.* rasterio==1.3.* rioxarray shapely pyproj fiona rtree \
        scikit-learn xarray netCDF4 sentinelsat cdsapi \
        google-cloud-storage earthengine-api geemap \
        #acolite==0.5.7  \
        trimesh pyvista \
        jupyterlab==4.*

# ---------- 4. Copy project code ----------
WORKDIR /workflow
COPY . /workflow

# ---------- 5. Earth-Engine CLI check ----------
# Users still need to authenticate *inside* the container once:
#   docker run …  →  earthengine authenticate --no_browser
RUN earthengine --version

# ---------- 6. Helpful docs for proxy + auth ----------
# Optional: user can uncomment the example lines below and rebuild to
# bake in proxy URLs if they are behind a corporate firewall.
#
# ARG HTTP_PROXY
# ARG HTTPS_PROXY
# ENV HTTP_PROXY=${HTTP_PROXY}
# ENV HTTPS_PROXY=${HTTPS_PROXY}

# ---------- 7. Default Jupyter entrypoint ----------
#  Jupyter token = reefmap   (change via env-var if you like)
EXPOSE 8888
ENV PYTHONPATH=/workflow
ENV JUPYTER_TOKEN=reefmap
CMD ["jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root"]

###############################################################################
#  How to run (cheat-sheet)
#
#  1.  Build (Intel / AMD)             :  docker build -t reefmap/mhp .
#      Build (Apple M-series)          :  docker build --platform=linux/amd64 -t reefmap/mhp .
#
#  2.  First run (Windows/mac/Linux) :
#      docker run -it --rm \
#          -p 8888:8888 \
#          -v "$PWD/data:/workflow/data" \
#          -v "$PWD/tiles:/workflow/tiles" \
#          -v "$PWD/outputs:/workflow/outputs" \
#          reefmap/mhp
#
#      # Inside the container, authenticate Earth-Engine once:
#      earthengine authenticate --no_browser
#
#      # (Optional) mount a GCP service-account key for Cloud workflow:
#      #   docker run -e GOOGLE_APPLICATION_CREDENTIALS=/secrets/key.json \
#      #              -v $HOME/key.json:/secrets/key.json:ro …
#
#      # (Optional) mount ~/.cdsapirc if you want auto ERA5-Wave downloads:
#      #   -v $HOME/.cdsapirc:/root/.cdsapirc:ro
#
#  3.  Open browser to  http://localhost:8888  (token: reefmap)
#      Run   `python run_pipeline.py`   and follow the prompts.
###############################################################################
