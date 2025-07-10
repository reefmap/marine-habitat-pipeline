import ee
from ee.batch import Export

def process_tile_cloud(tile_geom, tile_id, config):
    """
    Run the Clear-Water Mosaic for one tile entirely in the cloud.

    Args:
      tile_geom: a shapely geometry (in EPSG:4326) for the tile.
      tile_id:   unique string identifier for this tile.
      config:    dict from load_config(), must include:
        - start_date, end_date
        - cloud_thresh, chla_thresh, wind_thresh, max_scenes
        - gee_bucket, gee_folder, gee_scale

    Returns:
      The Earth Engine task ID string for the export.
    """
    # 1) Convert tile geometry to EE
    aoi_ee = ee.Geometry(tile_geom.__geo_interface__)

    # 2) Base Sentinel-2 Harmonized SR collection
    coll = (
        ee.ImageCollection('COPERNICUS/S2_SR_HARMONIZED')
          .filterDate(config['start_date'], config['end_date'])
          .filterBounds(aoi_ee)
          .filter(ee.Filter.lt('CLOUDY_PIXEL_PERCENTAGE', config['cloud_thresh']))
    )

    # 3) CHL-a mapping & filter
    if config.get('chla_thresh') is not None:
        chla_coll = (
            ee.ImageCollection('JAXA/GCOM-C/L3/OCEAN/CHLA/V3')
              .filterDate(config['start_date'], config['end_date'])
              .select('chlor_a')
        )
        def add_chla(img):
            t = ee.Date(img.get('system:time_start'))
            daily = chla_coll.filterDate(t, t.advance(1, 'day')).median()
            median = daily.reduceRegion(
                ee.Reducer.median(), aoi_ee, scale=1000
            ).get('chlor_a')
            return img.set('CHLA_MEDIAN', median)
        coll = coll.map(add_chla) \
                   .filter(ee.Filter.lt('CHLA_MEDIAN', config['chla_thresh']))

    # 4) Wind mapping & filter
    if config.get('wind_thresh') is not None:
        wind_coll = ee.ImageCollection('ECMWF/ERA5_LAND/HOURLY')
        def add_wind(img):
            t = ee.Date(img.get('system:time_start'))
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
            ).get('u10')
            return img.set('WIND_SPEED', speed)
        coll = coll.map(add_wind) \
                   .filter(ee.Filter.lt('WIND_SPEED', config['wind_thresh']))

    # 5) Apply max_scenes cap
    coll = coll.limit(config.get('max_scenes', 50))

    # 6) Water-occurrence mask (<10% occurrence = clear water)
    water = ee.Image('JRC/GSW1_4/GlobalSurfaceWater').select('occurrence')
    coll = coll.map(lambda img: img.updateMask(water.lt(10)))

    # 7) Median composite
    mosaic = coll.median().clip(aoi_ee)

    # 8) Export to Cloud Storage as a COG
    task = Export.image.toCloudStorage(
        image=mosaic,
        description=f'clearwater_{tile_id}',
        bucket=config['gee_bucket'],
        fileNamePrefix=f"{config.get('gee_folder','clearwater')}/{tile_id}",
        region=aoi_ee,
        scale=config.get('gee_scale', 10),
        crs='EPSG:4326',
        maxPixels=1e13,
        fileFormat='GeoTIFF',
        formatOptions={'cloudOptimized': True}
    )
    task.start()
    print(f"Started export for {tile_id}: task ID = {task.id}")
    return task.id
