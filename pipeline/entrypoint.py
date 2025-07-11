#!/usr/bin/env python3

import os
import sys
import pandas as pd
import geopandas as gpd

from .config import load_config
from .clearwater.tiler import load_aoi, buffer_aoi, split_aoi_to_tiles
from .clearwater.filter import filter_scenes
from .clearwater.estimate import estimate_resources
from .utils.gee_utils import initialize_ee

def choose_project():
    """Prompt or auto-detect the Earth Engine GCP project ID."""
    return (
        os.environ.get('EARTHENGINE_PROJECT')
        or input("Enter your Earth Engine GCP project ID (leave blank to use default): ").strip()
        or None
    )

def choose_config(config_path=None):
    """Prompt user for a config file if not provided."""
    config_path = config_path or os.environ.get('MHP_CONFIG')
    while not config_path or not os.path.isfile(config_path):
        print("Config file not found.")
        config_path = input("Enter the path to your config.json: ").strip()
    return config_path

def setup_ee(project):
    """Initialize Earth Engine with the specified project ID."""
    if project:
        os.environ['EARTHENGINE_PROJECT'] = project
    try:
        initialize_ee(project=project, interactive=False)
    except Exception:
        sys.exit(1)

def load_and_buffer(config):
    """Load AOI and apply buffer if requested."""
    aoi_path = config.get("aoi_path")
    if not aoi_path or not os.path.exists(aoi_path):
        print(f"AOI file not found: {aoi_path}")
        sys.exit(1)
    aoi_gdf = load_aoi(aoi_path)
    if config.get("buffer_aoi", False):
        aoi_gdf = buffer_aoi(aoi_gdf, buffer_km=config.get("buffer_km", 2))
    return aoi_gdf

def tile_aoi(aoi_gdf, config):
    """Split AOI into tiles."""
    tile_size = config.get('tile_size_km', 1)
    tiles_gdf = split_aoi_to_tiles(aoi_gdf, tile_size_km=tile_size)
    print(f"Split AOI into {len(tiles_gdf)} tiles (≈{tile_size} km each)")
    return tiles_gdf

def filter_tiles(tiles_gdf, config):
    """Run scene filtering and collect per-tile stats."""
    records = []
    for _, row in tiles_gdf.iterrows():
        tile_id = row.get('tile_id', None)
        if not tile_id:
            print("Error: tile_id missing!")
            continue
        print(f"\nProcessing tile {tile_id}…")
        tile_gdf = gpd.GeoDataFrame(geometry=[row.geometry], crs=tiles_gdf.crs)
        scenes = filter_scenes(
            tile_gdf,
            start_date   = config.get("start_date",   "2021-01-01"),
            end_date     = config.get("end_date",     "2021-12-31"),
            chla_thresh  = config.get("chla_thresh"),
            wind_thresh  = config.get("wind_thresh"),
            cloud_thresh = config.get("cloud_thresh", 20),
            max_scenes   = config.get("max_scenes",   50)
        )
        n = len(scenes)
        if n > 0:
            clouds = sorted(s['cloud'] for s in scenes if s.get('cloud') is not None)
            chlas  = sorted(s['chla']  for s in scenes if s.get('chla')  is not None)
            winds  = sorted(s['wind']  for s in scenes if s.get('wind')  is not None)
            median_cloud = clouds[len(clouds)//2] if clouds else None
            median_chla  = chlas[len(chlas)//2]   if chlas  else None
            median_wind  = winds[len(winds)//2]   if winds  else None
        else:
            median_cloud = median_chla = median_wind = None
        records.append({
            'tile_id':      tile_id,
            'n_scenes':     n,
            'median_cloud': median_cloud,
            'median_chla':  median_chla,
            'median_wind':  median_wind
        })
    return records

def print_tile_stats(records):
    stats_df = pd.DataFrame.from_records(records)
    print("\nTile-level scene statistics:")
    print(stats_df.to_string(index=False))

def estimate_and_decide(records, config):
    resources = estimate_resources(
        tile_stats         = records,
        avg_scene_size_gb  = config.get('scene_size_gb',      0.5),
        cpu_hours_per_tile = config.get('cpu_hours_per_tile', 0.1),
        storage_rate_per_gb= config.get('storage_cost_gb',    0.02),
        cpu_rate_per_hour  = config.get('compute_cost_hr',     0.05)
    )
    print("\nEstimated resources:")
    for k, v in resources.items():
        print(f"  {k}: {v}")
    is_cloud = resources['cpu_hours'] > config.get('max_offline_cpu_hours', 10)
    return resources, is_cloud

def run_cloud_exports(tiles_gdf, config):
    from .clearwater.cloud_runner import process_tile_cloud
    print("Launching GEE Cloud exports…")
    task_ids = []
    for _, row in tiles_gdf.iterrows():
        tid = process_tile_cloud(row.geometry, row.tile_id, config)
        task_ids.append(tid)
    print("Submitted tasks:", task_ids)
    print("\nCheck your Google Cloud Storage bucket for outputs. Monitor tasks in the GEE Tasks panel.")

def run_offline_exports(tiles_gdf, config):
    from .clearwater.offline_runner import process_tile_offline
    print("Running Offline processing…")
    for _, row in tiles_gdf.iterrows():
        process_tile_offline(row.geometry, row.tile_id, config)
    print("\nCheck the output directory for your offline results.")

def main(config_path: str = None):
    print("=== Marine Habitat Pipeline ===")
    config_path = choose_config(config_path)
    project = choose_project()
    setup_ee(project)
    config = load_config(config_path)
    print(f"Loaded config from {config['aoi_path']}")
    aoi_gdf = load_and_buffer(config)
    tiles_gdf = tile_aoi(aoi_gdf, config)
    records = filter_tiles(tiles_gdf, config)
    print_tile_stats(records)
    resources, is_cloud = estimate_and_decide(records, config)
    if is_cloud:
        print("\n► Recommend: RUN ON GEE CLOUD based on CPU estimate.")
        run_cloud_exports(tiles_gdf, config)
    else:
        print("\n► Recommend: RUN OFFLINE locally based on CPU estimate.")
        run_offline_exports(tiles_gdf, config)
    print("\nPipeline complete. See documentation for next analysis steps.")

if __name__ == "__main__":
    main(config_path=os.environ.get('MHP_CONFIG'))
