#!/usr/bin/env python3
"""
analysis_stage.py

Analysis stage for the Marine-Habitat pipeline.
"""

import argparse
import ee
import json
import sys
from pathlib import Path

# Import pipeline stages
from export_stage import export_to_gee
from download_stage import poll_and_download
from generate_acolite import run_acolite
from utils import load_aoi
from utils import setup_ee_credentials
from bathymetry import compute_bathymetry, compute_topobathy
from terrain import compute_slope, compute_bpi
from wave import compute_wave_layer
from stacking import stack_layers
from clustering import run_kmeans
from export_results import export_results

# Default GEBCO asset if none provided by user
DEFAULT_GEBCO = (
    "projects/earthengine-legacy/assets/WorldBathymetry/GEBCO_2023_15arcsec"
)


def _parse_args() -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(
        description="Run analysis stage of the Marine-Habitat pipeline."
    )
    parser.add_argument(
        "--config", "-c",
        required=True,
        help="Path to pipeline configuration file (config.json)"
    )
    parser.add_argument(
        "--gebco_asset",
        default=DEFAULT_GEBCO,
        help="Earth Engine asset ID for GEBCO bathymetry"
    )
    return parser.parse_args()


def _check_asset(asset_id: str) -> None:
    """Fail early if the user-supplied EE asset does not exist or is private."""
    try:
        ee.data.getAsset(asset_id)
    except ee.ee_exception.EEException as exc:
        raise ValueError(
            f"Earth Engine asset '{asset_id}' not found or not accessible.\n"
            "🔑  Make sure you imported GEBCO into your EE account or pass "
            "--gebco_asset pointing at a public asset."
        ) from exc


def run_full_analysis(config_path: Path, gebco_asset: str) -> None:
    """
    Load config and execute full pipeline:
      1. AOI load
      2. EE setup
      3. Export & download Sentinel-2 composite
      4. Apply ACOLITE
      5. Compute bathymetry and topobathymetry
      6. Derive slope & BPI
      7. Compute wave proxy
      8. Stack layers & cluster
      9. Export final results
    """
    # Load configuration
    with open(config_path, 'r') as f:
        config = json.load(f)

    # Setup Earth Engine credentials
    setup_ee_credentials()  # validates authentication
    ee.Initialize()

    # Load AOI geometry
    print("📐 Loading AOI from configuration...")
    aoi = load_aoi(Path(config["aoi_shapefile"]))

    # 1. Export to Earth Engine
    print("☁️  Exporting Sentinel-2 composite to EE...")
    export_task = export_to_gee(aoi, config)

    # 2. Poll and download results
    print("🔄 Polling export task and downloading composites...")
    composite_path = poll_and_download(export_task, config)

    # 3. Local ACOLITE processing
    print("🌊 Running ACOLITE DSF processing locally...")
    dsf_path = run_acolite(composite_path, config)

    # 4. Bathymetry & topobathymetry
    print("🌐 Computing bathymetry and topobathymetry...")
    bathy = compute_bathymetry(dsf_path, gebco_asset, config)
    topo = compute_topobathy(dsf_path, bathy, config)

    # 5. Terrain derivatives
    print("⛰️  Deriving slope and BPI...")
    slope = compute_slope(topo, config)
    bpi = compute_bpi(topo, config)

    # 6. Wave proxy layer
    print("💨 Computing wave proxy layer...")
    wave = compute_wave_layer(aoi, config)

    # 7. Stack and cluster
    print("🗄️  Stacking layers and running clustering...")
    stack_path = stack_layers([bathy, slope, bpi, wave], config)
    clusters = run_kmeans(stack_path, config.get("n_clusters", 5), config)

    # 8. Export final clustered map and metrics
    print("📤 Exporting final results...")
    export_results(clusters, config)

    print("🎉 Analysis complete. Outputs available in", config.get("output_dir", "./outputs"))


def main(cfg_path: str | Path | None = None) -> None:
    """
    CLI entry point for analysis_stage.
    """
    args = _parse_args() if cfg_path is None else None
    config_file = Path(cfg_path or args.config)
    gebco_asset = args.gebco_asset if args else DEFAULT_GEBCO

    if not config_file.exists():
        raise FileNotFoundError(f"Configuration file '{config_file}' not found.")

    _check_asset(gebco_asset)
    run_full_analysis(config_file, gebco_asset)


if __name__ == "__main__":
    sys.exit(main())
