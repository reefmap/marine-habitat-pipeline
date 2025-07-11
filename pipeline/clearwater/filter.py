# pipeline/clearwater/filter.py
# ---------------------------------------------------------------------
import ee

# ---------------------------------------------------------------------
_FILL = -9999            # placeholder for “null” values from EE


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
    max_scenes=50,
):
    """
    Return a list[dict] with keys id / date / cloud / chla / wind for
    Sentinel-2 SR-HARMONIZED scenes that meet every threshold.

    Assumes ee.Initialize() has already been called.
    """
    # AOI → ee.Geometry
    aoi_ee = ee.Geometry(
        aoi_gdf.to_crs(4326).geometry.unary_union.__geo_interface__
    )

    # 1) Sentinel-2 base collection
    coll = (
        ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED")
        .filterDate(start_date, end_date)
        .filterBounds(aoi_ee)
        .filter(ee.Filter.lt("CLOUDY_PIXEL_PERCENTAGE", cloud_thresh))
    )

    # ------------------------------------------------------------------
    # 2) CHL-a filter  (JAXA GCOM-C V3 – 8-day product)
    if chla_thresh is not None:
        chla_coll = (
            ee.ImageCollection("JAXA/GCOM-C/L3/OCEAN/CHLA/V3")
            .filterDate(start_date, end_date)
        )

        def add_chla(img):
            d   = ee.Date(img.get("system:time_start"))
            day = chla_coll.filterDate(d, d.advance(1, "day")).median()

            # Work out which band name is present once per image
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
                scale     = 4500,        # native ≈4.6 km
                bestEffort=True,
            ).get(band)

            return img.set("CHLA_MEDIAN", _safe_set(median))

        coll = (
            coll.map(add_chla)
            .filter(ee.Filter.lt("CHLA_MEDIAN", chla_thresh))
        )

    # ------------------------------------------------------------------
    # 3) Wind speed  – use NOAA Pathfinder v5.3 (12-hourly, 4 km, 1981-present)
    if wind_thresh is not None:
        # Only keep Pathfinder pixels that are “acceptable-quality” or better
        pth = (ee.ImageCollection("NOAA/CDR/SST_PATHFINDER/V53")
               .filterDate(start_date, end_date)
               .map(lambda img:
                    img.updateMask(img.select("quality_level").gte(4)))
               .select("wind_speed"))

        def add_wind(img):
            t = ee.Date(img.get("system:time_start"))
            # Pick the Pathfinder scene closest in time (±6 h)
            wind = (pth.filterDate(t.advance(-6, "hour"), t.advance(6, "hour"))
                    .first())
            speed = wind.reduceRegion(
                reducer=ee.Reducer.mean(),
                geometry=aoi_ee,
                scale=4000,  # native pixel size
                bestEffort=True
            ).get("wind_speed")
            return img.set("WIND_SPEED", _safe_set(speed))

        coll = coll.map(add_wind).filter(ee.Filter.lt("WIND_SPEED", wind_thresh))

        # ------------------------------------------------------------------

    # 4) Limit the number of scenes (after all filters)
    if max_scenes:
        coll = coll.limit(max_scenes)

    # ------------------------------------------------------------------
    # 5) Bring metadata back to Python (single round-trip each)
    ids     = coll.aggregate_array("system:index").getInfo()
    times   = coll.aggregate_array("system:time_start").getInfo()
    clouds  = coll.aggregate_array("CLOUDY_PIXEL_PERCENTAGE").getInfo()
    chlas   = (
        coll.aggregate_array("CHLA_MEDIAN").getInfo()
        if chla_thresh is not None else [None] * len(ids)
    )
    winds   = (
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
