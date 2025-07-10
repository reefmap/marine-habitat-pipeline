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

def main(config_path: str = None):
    # 1) Prompt for Earth Engine project if not already set
    ee_project = (
        os.environ.get('EARTHENGINE_PROJECT')
        or input("Enter your Earth Engine GCP project ID (leave blank to use default): ").strip()
        or None
    )
    if ee_project:
        os.environ['EARTHENGINE_PROJECT'] = ee_project

    # 2) Initialize Earth Engine
    try:
        initialize_ee(project=ee_project, interactive=False)
    except Exception:
        sys.exit(1)

    print("=== Marine Habitat Pipeline ===")
    config = load_config(config_path)
    print(f"Loaded config from {config['aoi_path']}")

    # 3) Load and optionally buffer AOI
    aoi_gdf = load_aoi(config["aoi_path"])
    if config.get("buffer_aoi", False):
        aoi_gdf = buffer_aoi(aoi_gdf, buffer_km=config.get("buffer_km", 2))

    # 4) Split AOI into tiles
    tile_size = config.get('tile_size_km', 1)
    tiles_gdf = split_aoi_to_tiles(aoi_gdf, tile_size_km=tile_size)
    print(f"Split AOI into {len(tiles_gdf)} tiles (≈{tile_size} km each)")

    # 5) Per-tile scene filtering and stats collection
    records = []
    for _, row in tiles_gdf.iterrows():
        tile_id = row['tile_id']
        print(f"\nProcessing tile {tile_id}…")

        # Wrap geometry for filter_scenes
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

    # 6) Show per-tile stats
    stats_df = pd.DataFrame.from_records(records)
    print("\nTile-level scene statistics:")
    print(stats_df.to_string(index=False))

    # 7) Estimate resource needs
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

    # 8) Decide cloud vs offline and execute
    if resources['cpu_hours'] > config.get('max_offline_cpu_hours', 10):
        # Cloud branch
        print("\n► Recommend: RUN ON GEE CLOUD based on CPU estimate.")
        from .clearwater.cloud_runner import process_tile_cloud

        print("Launching GEE Cloud exports…")
        task_ids = []
        for _, row in tiles_gdf.iterrows():
            tid = process_tile_cloud(row.geometry, row.tile_id, config)
            task_ids.append(tid)
        print("Submitted tasks:", task_ids)

    else:
        # Offline branch
        print("\n► Recommend: RUN OFFLINE locally based on CPU estimate.")
        from .clearwater.offline_runner import process_tile_offline

        print("Running Offline processing…")
        for _, row in tiles_gdf.iterrows():
            process_tile_offline(row.geometry, row.tile_id, config)

if __name__ == "__main__":
    # If called directly, allow optional --config via env var or default
    main(config_path=os.environ.get('MHP_CONFIG'))
