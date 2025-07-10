import geopandas as gpd
from shapely.geometry import box
from itertools import product

def load_aoi(aoi_path):
    """
    Load an AOI from file into a GeoDataFrame (EPSG:4326),
    report its area in km², and return.
    """
    gdf = gpd.read_file(aoi_path)
    if gdf.crs is None:
        gdf.set_crs(epsg=4326, inplace=True)
    # Compute area for reporting
    gdf_proj = gdf.to_crs(epsg=3857)
    area_km2 = gdf_proj.geometry.area.sum() / 1e6
    print(f"Loaded AOI: {aoi_path} ({area_km2:.2f} km²)")
    return gdf

def buffer_aoi(gdf, buffer_km=2):
    """
    Buffer an AOI GeoDataFrame by buffer_km (in meters),
    reprojecting back to the original CRS.
    """
    gdf_proj = gdf.to_crs(epsg=3857)
    buffered = gdf_proj.geometry.buffer(buffer_km * 1000)
    # Wrap back into a GeoDataFrame and reproject
    buf_gdf = gpd.GeoDataFrame(
        geometry=buffered, crs=gdf_proj.crs
    ).to_crs(gdf.crs)
    print(f"Buffered AOI by {buffer_km} km")
    return buf_gdf

def split_aoi_to_tiles(aoi_gdf: gpd.GeoDataFrame, tile_size_km: float = 1.0) -> gpd.GeoDataFrame:
    """
    Split an AOI GeoDataFrame into square tiles of size tile_size_km.

    Returns a GeoDataFrame in EPSG:4326 with columns:
      - tile_id: "tile_<col>_<row>"
      - geometry: the intersection of each tile with the AOI
    """
    # 1) Project to Web Mercator so that distances are in meters
    merc = aoi_gdf.to_crs(epsg=3857)
    minx, miny, maxx, maxy = merc.total_bounds

    size_m = tile_size_km * 1000.0
    cols = int((maxx - minx) // size_m) + 1
    rows = int((maxy - miny) // size_m) + 1

    # 2) Generate the grid of boxes
    tiles = []
    for i, j in product(range(cols), range(rows)):
        x0 = minx + i * size_m
        y0 = miny + j * size_m
        tile_geom = box(x0, y0, x0 + size_m, y0 + size_m)
        tiles.append({
            'tile_id': f'tile_{i:03d}_{j:03d}',
            'geometry': tile_geom
        })

    # 3) Make a GeoDataFrame of all tiles and intersect with the AOI
    tiles_gdf = gpd.GeoDataFrame(tiles, crs=3857)
    intersected = gpd.overlay(tiles_gdf, merc, how='intersection')

    # 4) Reproject back to WGS84 (EPSG:4326) and return only tile_id & geometry
    result = intersected.to_crs(epsg=4326)[['tile_id', 'geometry']]
    print(f"Split AOI into {len(result)} tiles (~{tile_size_km} km each)")
    return result
