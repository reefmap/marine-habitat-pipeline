import ee
import geemap

def filter_scenes(aoi_gdf, start_date, end_date, chla_thresh=None, cloud_thresh=20, wind_thresh=7):
    """
    Filters Sentinel-2 scenes over AOI for clear water mosaic.
    - aoi_gdf: Buffered AOI as GeoDataFrame
    - start_date, end_date: Strings ('YYYY-MM-DD')
    - chla_thresh, cloud_thresh, wind_thresh: Filtering thresholds
    Returns: List of filtered scene IDs or metadata
    """
    try:
        ee.Initialize()
    except Exception as e:
        print("ERROR: Google Earth Engine is not initialized.")
        print("• Ensure your account is signed up at: https://signup.earthengine.google.com/")
        print("• If using a service account or custom project, see: https://developers.google.com/earth-engine/guides/access")
        raise e

    # Convert AOI to EE geometry
    aoi_json = aoi_gdf.to_crs(4326).geometry.unary_union.__geo_interface__
    aoi_ee = ee.Geometry(aoi_json)

    # Sentinel-2 surface reflectance
    s2 = ee.ImageCollection('COPERNICUS/S2_SR') \
            .filterDate(start_date, end_date) \
            .filterBounds(aoi_ee)
    # Cloud filter (example using QA60 band)
    s2 = s2.filter(ee.Filter.lt('CLOUDY_PIXEL_PERCENTAGE', cloud_thresh))
    # TODO: Add CHL-a, wind, tide filters here

    # Get list of scene info
    scene_list = s2.aggregate_array('system:index').getInfo()
    print(f"Found {len(scene_list)} filtered scenes.")
    return scene_list
