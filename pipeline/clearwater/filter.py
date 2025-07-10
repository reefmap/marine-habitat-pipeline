import ee
import geemap

def filter_scenes(aoi_gdf, start_date, end_date,
                  chla_thresh=None, cloud_thresh=20,
                  wind_thresh=None, max_scenes=50):
    """
    Filters Sentinel-2 scenes over AOI for clear water mosaic, incorporating optional CHL-a and wind speed thresholds.

    Args:
        aoi_gdf: GeoPandas GeoDataFrame containing the AOI geometry (in EPSG:4326).
        start_date: Start date as 'YYYY-MM-DD'.
        end_date: End date as 'YYYY-MM-DD'.
        chla_thresh: Optional maximum chlorophyll-a concentration (mg/m^3).
        cloud_thresh: Maximum CLOUDY_PIXEL_PERCENTAGE (default 20).
        wind_thresh: Optional maximum wind speed (m/s).
        max_scenes: Maximum number of scenes to return.

    Returns:
        A list of dicts, each with keys: 'id', 'date', 'cloud', 'chla', 'wind'.
    """
    # ASSUME Earth Engine was already initialized

    # Convert AOI GeoDataFrame to EE Geometry
    aoi_geojson = aoi_gdf.to_crs(4326).geometry.unary_union.__geo_interface__
    aoi_ee = ee.Geometry(aoi_geojson)

    # 1) Base Sentinel-2 Harmonized SR collection
    coll = (
        ee.ImageCollection('COPERNICUS/S2_SR_HARMONIZED')
          .filterDate(start_date, end_date)
          .filterBounds(aoi_ee)
          .filter(ee.Filter.lt('CLOUDY_PIXEL_PERCENTAGE', cloud_thresh))
    )

    # 2) CHL-a filter
    if chla_thresh is not None:
        chla_coll = (
            ee.ImageCollection('JAXA/GCOM-C/L3/OCEAN/CHLA/V3')
              .filterDate(start_date, end_date)
              .select('chlor_a')
        )
        def add_chla(image):
            t = ee.Date(image.get('system:time_start'))
            daily = chla_coll.filterDate(t, t.advance(1, 'day')).median()
            median = daily.reduceRegion(
                ee.Reducer.median(), aoi_ee, scale=1000
            ).get('chlor_a')
            return image.set('CHLA_MEDIAN', median)
        coll = coll.map(add_chla).filter(ee.Filter.lt('CHLA_MEDIAN', chla_thresh))

    # 3) Wind filter
    if wind_thresh is not None:
        wind_coll = ee.ImageCollection('ECMWF/ERA5_LAND/HOURLY')
        def add_wind(image):
            t = ee.Date(image.get('system:time_start'))
            wind_img = (
                wind_coll.filterDate(t, t.advance(1, 'day'))
                         .select(['u10', 'v10'])
                         .mean()
            )
            speed = wind_img.expression(
                'sqrt(u10*u10 + v10*v10)',
                {
                    'u10': wind_img.select('u10'),
                    'v10': wind_img.select('v10')
                }
            ).reduceRegion(
                ee.Reducer.mean(), aoi_ee, scale=1000
            ).get('u10')  # ERA5 returns mean under band name
            return image.set('WIND_SPEED', speed)
        coll = coll.map(add_wind).filter(ee.Filter.lt('WIND_SPEED', wind_thresh))

    # 4) Cap the number of scenes
    if max_scenes is not None:
        coll = coll.limit(max_scenes)

    # 5) Retrieve filtered collection metadata
    info = coll.getInfo()  # returns dict with 'features'
    features = info.get('features', [])

    # 6) Build the Python list of scene dicts
    scenes = []
    for feat in features:
        props = feat.get('properties', {})
        scenes.append({
            'id':    props.get('system:index'),
            'date':  props.get('system:time_start'),
            'cloud': props.get('CLOUDY_PIXEL_PERCENTAGE'),
            'chla':  props.get('CHLA_MEDIAN'),
            'wind':  props.get('WIND_SPEED')
        })

    print(f"Found {len(scenes)} filtered scenes.")
    return scenes
