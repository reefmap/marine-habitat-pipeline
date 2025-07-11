# pipeline/clearwater/filter.py
# ---------------------------------------------------------------------
# External services & assets referenced here
# • Google Earth Engine SDK  (earthengine-api)
# • Sentinel-2 SR-HARMONIZED         COPERNICUS/S2_SR_HARMONIZED
# • CHL-a 8-day product              JAXA/GCOM-C/L3/OCEAN/CHLA/V3
# • ERA5 1-hourly reanalysis         ECMWF/ERA5/HOURLY
# • Global tidal-range raster        users/<YOU>/global_tidal_range
# ---------------------------------------------------------------------
import ee
from .tide import tide_ok          # <-- new

# ---------------------------------------------------------------------
_FILL = -9999          # placeholder for “null” values returned by EE


def _safe_set(val):
    """Return ee.Number(val) or the _FILL sentinel if val is null."""
    return ee.Algorithms.If(
        ee.Algorithms.IsEqual(val, None),
        ee.Number(_FILL),
        val,
    )


def _to_python(val):
    """Convert EE placeholder back to Python None."""
    return None if val in (_FILL, None) else val


# ---------------------------------------------------------------------
def filter_scenes(
    aoi_gdf,
    start_date,
    end_date,
    *,
    chla_thresh=None,
    cloud_thresh=20,
    wind_thresh=None,
    tidal_thresh=None,          
    max_scenes=50,
):
    """
    Return list[dict] with keys id / date / cloud / chla / wind for every
    Sentinel-2 SR scene that passes the thresholds.

    Requires ee.Initialize() to be called before you enter.
    """
    # AOI → ee.Geometry
    aoi_ee = ee.Geometry(
        aoi_gdf.to_crs(4326).geometry.unary_union.__geo_interface__
    )

    # 1) Sentinel-2 base collection  -----------------------------------
    coll = (
        ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED")
        .filterDate(start_date, end_date)
        .filterBounds(aoi_ee)
        .filter(ee.Filter.lt("CLOUDY_PIXEL_PERCENTAGE", cloud_thresh))
        .filter(tide_ok(aoi_ee, thresh_m=tidal_thresh))          
    )

    # 2) CHL-a filter  (JAXA GCOM-C V3 – 8-day)  -----------------------
    if chla_thresh is not None:
        chla_coll = (
            ee.ImageCollection("JAXA/GCOM-C/L3/OCEAN/CHLA/V3")
            .filterDate(start_date, end_date)
        )

        def add_chla(img):
            d   = ee.Date(img.get("system:time_start"))
            day = chla_coll.filterDate(d, d.advance(1, "day")).median()

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
                scale     = 4_500,          # native ≈4.6 km
                bestEffort=True,
            ).get(band)

            return img.set("CHLA_MEDIAN", _safe_set(median))

        coll = (
            coll.map(add_chla)
            .filter(ee.Filter.lt("CHLA_MEDIAN", chla_thresh))
        )

    # 3) Wind speed filter  (ERA5 HOURLY, 0.25°)  ----------------------
    if wind_thresh is not None:
        era5 = (
            ee.ImageCollection("ECMWF/ERA5/HOURLY")
            .filterDate(start_date, end_date)
            .select(["u_component_of_wind_10m", "v_component_of_wind_10m"])
        )

        def mag(img):
            """Return img with a single band 'wind' = √(u²+v²)."""
            w = img.expression(
                "sqrt(u*u + v*v)",
                {
                    "u": img.select("u_component_of_wind_10m"),
                    "v": img.select("v_component_of_wind_10m"),
                },
            ).rename("wind")
            return w.copyProperties(img, img.propertyNames())

        era5 = era5.map(mag)

        def add_wind(img):
            t = ee.Date(img.get("system:time_start"))
            wind = (
                era5.filterDate(t.advance(-3, "hour"), t.advance(3, "hour"))
                .median()
            )

            speed = wind.select("wind").reduceRegion(
                reducer=ee.Reducer.mean(),
                geometry=aoi_ee,
                scale=25_000,           # ~0.25° at equator
                bestEffort=True,
            ).get("wind")

            return img.set("WIND_SPEED", _safe_set(speed))

        coll = (
            coll.map(add_wind)
            .filter(ee.Filter.lt("WIND_SPEED", wind_thresh))
        )

    # 4) Cap the number of scenes  -------------------------------------
    if max_scenes:
        coll = coll.limit(max_scenes)

    # 5) Bring metadata back to Python  --------------------------------
    ids    = coll.aggregate_array("system:index").getInfo()
    times  = coll.aggregate_array("system:time_start").getInfo()
    clouds = coll.aggregate_array("CLOUDY_PIXEL_PERCENTAGE").getInfo()
    chlas  = (
        coll.aggregate_array("CHLA_MEDIAN").getInfo()
        if chla_thresh is not None else [None] * len(ids)
    )
    winds  = (
        coll.aggregate_array("WIND_SPEED").getInfo()
        if wind_thresh is not None else [None] * len(ids)
    )

    scenes = [
        {
            "id":    sid,
            "date":  ts,
            "cloud": cl,
            "chla":  _to_python(ch),
            "wind":  _to_python(w),
        }
        for sid, ts, cl, ch, w in zip(ids, times, clouds, chlas, winds)
    ]

    print(f"Found {len(scenes)} filtered scenes.")
    return scenes
# ---------------------------------------------------------------------
