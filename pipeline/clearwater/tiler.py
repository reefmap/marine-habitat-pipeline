import geopandas as gpd

def load_aoi(aoi_path):
    gdf = gpd.read_file(aoi_path)
    if gdf.crs is None:
        gdf.set_crs(epsg=4326, inplace=True)
    gdf_proj = gdf.to_crs(3857)  # meters
    area_km2 = gdf_proj.area.sum() / 1e6
    print(f"Loaded AOI: {aoi_path} ({area_km2:.2f} km²)")
    return gdf

def buffer_aoi(gdf, buffer_km=2):
    gdf_proj = gdf.to_crs(3857)  # meters
    gdf_buffer = gdf_proj.buffer(buffer_km * 1000)
    gdf_buffer = gpd.GeoDataFrame(geometry=gdf_buffer, crs=gdf_proj.crs).to_crs(gdf.crs)
    print(f"Buffered AOI by {buffer_km} km")
    return gdf_buffer