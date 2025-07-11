# pipeline/clearwater/cloud_runner.py
# ---------------------------------------------------------------------
# External services & assets
#   • EE Sentinel-2 SR-HARMONIZED   COPERNICUS/S2_SR_HARMONIZED
#   • EE CHL-a 8-day product        JAXA/GCOM-C/L3/OCEAN/CHLA/V3
#   • EE ERA5 hourly reanalysis     ECMWF/ERA5/HOURLY
#   • EE Global water occurrence    JRC/GSW1_4/GlobalSurfaceWater
#   • EE Global tidal range raster  users/<YOU>/global_tidal_range
# ---------------------------------------------------------------------
import time
import ee
from ee.batch import Export
from .tide import tide_ok

# ---------------------------------------------------------------------
MAX_ACTIVE = 250     # leave headroom under EE’s 300-task limit

def _throttle():
    while len(ee.data.getTaskList()) >= MAX_ACTIVE:
        print("GEE task queue full — sleeping 60 s …")
        time.sleep(60)

# ---------------------------------------------------------------------
def process_tile_cloud(tile_geom, tile_id, config):
    """
    Build the clear-water composite for one tile entirely in Earth Engine.

    Args
    ----
    tile_geom : shapely geometry (EPSG:4326) of the tile
    tile_id   : unique string ID
    config    : dict with at least
        start_date, end_date
        cloud_thresh, chla_thresh, wind_thresh, tidal_thresh_m
        max_scenes, water_occurrence_thresh
        gee_bucket, gee_folder, gee_scale

    Returns
    -------
    str  Earth Engine task ID
    """
    # 1) AOI ➜ EE geometry
    aoi_ee = ee.Geometry(tile_geom.__geo_interface__)

    # 2) Sentinel-2 collection with cloud + tide filters
    coll = (
        ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED")
        .filterDate(config["start_date"], config["end_date"])
        .filterBounds(aoi_ee)
        .filter(ee.Filter.lt("CLOUDY_PIXEL_PERCENTAGE", config["cloud_thresh"]))
        .filter(tide_ok(aoi_ee, thresh_m=config.get("tidal_thresh_m")))
    )

    # 3) CHL-a median per scene (JAXA GCOM-C V3, 8-day)
    if config.get("chla_thresh") is not None:
        chla_src = (
            ee.ImageCollection("JAXA/GCOM-C/L3/OCEAN/CHLA/V3")
            .filterDate(config["start_date"], config["end_date"])
        )

        def add_chla(img):
            d   = ee.Date(img.get("system:time_start"))
            day = chla_src.filterDate(d, d.advance(1, "day")).median()

            band = ee.String(
                ee.Algorithms.If(
                    day.bandNames().contains("CHLA_AVE"),
                    "CHLA_AVE",
                    "chlor_a",
                )
            )

            median = day.select(band).reduceRegion(
                reducer   = ee.Reducer.median(),
                geometry  = aoi_ee,
                scale     = 4_500,            # native ≈ 4.6 km
                bestEffort=True,
            ).get(band)

            return img.set("CHLA_MEDIAN", median)

        coll = (
            coll.map(add_chla)
            .filter(ee.Filter.lt("CHLA_MEDIAN", config["chla_thresh"]))
        )

    # 4) Wind filter (ERA5 hourly, 10 m)
    if config.get("wind_thresh") is not None:
        era5 = (
            ee.ImageCollection("ECMWF/ERA5/HOURLY")
            .filterDate(config["start_date"], config["end_date"])
            .select(["u_component_of_wind_10m", "v_component_of_wind_10m"])
        )

        def add_wind(img):
            t = ee.Date(img.get("system:time_start"))
            wind = (
                era5.filterDate(t.advance(-3, "hour"), t.advance(3, "hour"))
                .median()
            )
            speed = wind.expression(
                "sqrt(u*u + v*v)",
                {
                    "u": wind.select("u_component_of_wind_10m"),
                    "v": wind.select("v_component_of_wind_10m"),
                },
            ).rename("wind")

            mean = speed.reduceRegion(
                ee.Reducer.mean(), aoi_ee, scale=25_000, bestEffort=True
            ).get("wind")

            return img.set("WIND_SPEED", mean)

        coll = (
            coll.map(add_wind)
            .filter(ee.Filter.lt("WIND_SPEED", config["wind_thresh"]))
        )

    # 5) Cap number of scenes
    coll = coll.limit(config.get("max_scenes", 50))

    # 6) Water-occurrence mask  (keep pixels ≥ threshold %)
    water = ee.Image("JRC/GSW1_4/GlobalSurfaceWater").select("occurrence")
    thresh = config.get("water_occurrence_thresh", 80)
    wmask  = water.gte(thresh)
    coll   = coll.map(lambda img: img.updateMask(wmask))

    # 7) Median composite
    mosaic = coll.median().clip(aoi_ee)

    # 8) Export COG ➜ GCS
    _throttle()     # be polite before adding a new task
    task = Export.image.toCloudStorage(
        image        = mosaic,
        description  = f"clearwater_{tile_id}",
        bucket       = config["gee_bucket"],
        fileNamePrefix = f"{config.get('gee_folder', 'clearwater')}/{tile_id}",
        region       = aoi_ee,
        scale        = config.get("gee_scale", 10),
        crs          = "EPSG:4326",
        maxPixels    = 1e13,
        fileFormat   = "GeoTIFF",
        formatOptions= {"cloudOptimized": True},
    )
    task.start()
    print(f"Started export for {tile_id}: task ID = {task.id}")
    return task.id
# ---------------------------------------------------------------------
