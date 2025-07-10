#!/usr/bin/env python3

import os
import sys
import argparse

from pipeline.entrypoint import main

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run Marine Habitat Pipeline")
    parser.add_argument(
        "-c","--config",
        default="data/config.json",
        help="Path to your pipeline config JSON (default: data/config.json)"
    )
    args = parser.parse_args()

    # Pre-set the config path and EE project if needed
    config_path = args.config
    if not os.path.exists(config_path):
        print(f"Error: Config file not found: {config_path}", file=sys.stderr)
        sys.exit(1)

    # You can also pick up EE project from env var EARTHENGINE_PROJECT here if desired

    # Call into your entrypoint, passing the config path
    main(config_path=config_path)
