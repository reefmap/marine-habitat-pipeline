#!/usr/bin/env python3

import json
from pipeline.config import load_config
from pipeline.clearwater.tiler import load_aoi, buffer_aoi
from pipeline.clearwater import filter as cw_filter

def main():
    print("=== Marine Habitat Pipeline ===")
    config = load_config()
    print(f"Loaded config from {config['aoi_path']}")
    aoi = load_aoi(config["aoi_path"])
    if config.get("buffer_aoi", False):
        aoi = buffer_aoi(aoi, buffer_km=config.get("buffer_km", 2))
    
    # Scene filtering step
    start_date = config.get("start_date", "2021-01-01")
    end_date = config.get("end_date", "2021-12-31")
    cloud_thresh = config.get("cloud_thresh", 20)
    # Add other thresholds as needed
    
    scenes = cw_filter.filter_scenes(
        aoi,
        start_date=start_date,
        end_date=end_date,
        cloud_thresh=cloud_thresh
        # add more args as needed
    )

    print(f"Filtered scene count: {len(scenes)}")
    print(scenes)

if __name__ == "__main__":
    main()
