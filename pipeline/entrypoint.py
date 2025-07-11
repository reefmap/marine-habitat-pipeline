# pipeline/entrypoint.py
# ---------------------------------------------------------------------
"""
Unified CLI for the marine-habitat pipeline.

Example
-------
python -m pipeline.entrypoint clearwater \
       --aoi reef.geojson \
       --start 2024-06-01 --end 2024-06-30 \
       --out s3://reefmap/mosaics/2024/ \
       --gee-service-account gee-sa.json \
       --copernicus-user you@example.com --copernicus-pass **** \
       --non-interactive
"""
# ---------------------------------------------------------------------
from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
import textwrap
import pathlib
import urllib.request
import urllib.parse
from datetime import datetime as _dt
from typing import List, Dict

import geopandas as gpd
import shapely.geometry as _shp
import ee

from .common.autodata import ensure_tidal_asset
from .clearwater import tiler, cloud_runner, offline_runner, estimate
from .clearwater.s2_fetch import fetch_scenes

# ---------------------------------------------------------------------
DEF_CFG: Dict[str, float | int] = {
    "cloud_thresh": 20,
    "chla_thresh": 0.3,
    "wind_thresh": 4.5,
    "tidal_thresh_m": 0.5,
    "water_occurrence_thresh": 80,
    "max_scenes": 50,
    "gee_scale": 10,
    # Cost-model knobs (used by resource estimator) -------------------
    "scene_size_gb": 0.55,
    "cpu_hours_per_tile": 0.12,
    "storage_cost_gb": 0.023,  # $/GB-month
    "compute_cost_hr": 0.048,  # $/vCPU-h
}

# ---------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------

def _load_aoi(aoi_str: str) -> gpd.GeoDataFrame:
    """Load AOI from path, URL or inline GeoJSON."""
    aoi_str = aoi_str.strip()
    # inline GeoJSON
    if aoi_str.startswith("{"):
        return gpd.GeoDataFrame.from_features(json.loads(aoi_str))
    # remote resource
    parsed = urllib.parse.urlparse(aoi_str)
    if parsed.scheme in ("http", "https", "ftp"):
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".geojson")
        tmp.write(urllib.request.urlopen(aoi_str).read())
        tmp.close()
        return gpd.read_file(tmp.name)
    # local file
    return gpd.read_file(aoi_str)


def _decide_mode(tile_stats: List[dict], cfg: dict, force: str | None) -> str:
    """Return either *cloud* or *offline* based on estimator (or --force)."""
    if force:
        return force
    est = estimate.estimate_resources(
        tile_stats,
        avg_scene_size_gb=cfg["scene_size_gb"],
        cpu_hours_per_tile=cfg["cpu_hours_per_tile"],
        storage_rate_per_gb=cfg["storage_cost_gb"],
        cpu_rate_per_hour=cfg["compute_cost_hr"],
    )
    return "cloud" if est["storage_gb"] > 50 or est["cpu_hours"] > 4 else "offline"

# ---------------------------------------------------------------------
# Clearwater lane entry-point
# ---------------------------------------------------------------------

def clearwater(args: argparse.Namespace) -> None:
    """Run Lane 1 (clear-water mosaic) from parsed CLI *args*."""
    # ---------- 1. Earth-Engine login ----------
    if args.gee_service_account:
        sa_key = pathlib.Path(args.gee_service_account)
        sa_email = json.loads(sa_key.read_text())["client_email"]
        creds = ee.ServiceAccountCredentials(sa_email, sa_key)
        ee.Initialize(credentials=creds)
    else:
        ee.Initialize()  # uses ~/.config/earthengine

    # Ensure tidal-range raster asset exists (lazy import inside fn).
    ensure_tidal_asset()

    # ---------- 2. AOI & tiling ----------
    aoi = _load_aoi(args.aoi)
    tiles = tiler.split_aoi(aoi, km=1)

    # ---------- 3. Build run-time config ----------
    cfg_path = pathlib.Path(args.config or "config.json")
    cfg = DEF_CFG.copy()
    if cfg_path.exists():
        cfg.update(json.loads(cfg_path.read_text()))
    cfg.update({
        "start_date": args.start,
        "end_date": args.end,
        "gee_bucket": (
            args.out.split("/", 3)[2] if args.out.startswith("s3://") else args.out
        ),
        "gee_folder": (
            "/".join(args.out.split("/", 3)[3:]) if args.out.startswith("s3://") else ""
        ),
    })
    cfg_path.write_text(json.dumps(cfg, indent=2))

    # ---------- 4. Decide cloud / offline ----------
    tile_stats = [{"n_scenes": cfg["max_scenes"]} for _ in tiles]
    mode = _decide_mode(tile_stats, cfg, args.force)
    print(f"▶ Running Lane-1 in **{mode.upper()}** mode")

    # ---------- 5. Process tiles ----------
    task_ids: List[str] = []
    for idx, tile in enumerate(tiles):
        geom = _shp.shape(tile["geometry"].__geo_interface__)
        tid = f"{tile['id']}_{idx:02}"

        if mode == "cloud":
            tid_out = cloud_runner.process_tile_cloud(geom, tid, cfg)
        else:
            fetch_scenes(
                aoi_wkt=geom.wkt,
                start=cfg["start_date"],
                end=cfg["end_date"],
                user=args.copernicus_user,
                pwd=args.copernicus_pass,
                outdir=pathlib.Path(args.out) / "scenes",
            )
            tid_out = offline_runner.process_tile_offline(geom, tid, cfg)
        task_ids.append(tid_out)

    print("Tiles kicked off:", task_ids)

# ---------------------------------------------------------------------
# CLI parser / main
# ---------------------------------------------------------------------

def _cli() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="mhp",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=textwrap.dedent(
            """Marine-Habitat Pipeline CLI
            
            Examples
            --------
            mhp clearwater --aoi reef.geojson --start 2024-06-01 --end 2024-06-30 \
               --out /tmp/mosaic --non-interactive
            """
        ),
    )
    sub = p.add_subparsers(dest="command", required=True)

    cw = sub.add_parser("clearwater", help="Lane-1 clear-water mosaic")
    cw.add_argument("--aoi", required=True, help="File path, URL or raw GeoJSON")
    cw.add_argument("--start", required=True)
    cw.add_argument("--end", required=True)
    cw.add_argument("--out", required=True, help="Local dir OR s3://bucket/prefix")
    cw.add_argument("--config", help="Existing config.json to load / overwrite")
    cw.add_argument("--gee-service-account", help="Path to service-account JSON key")
    cw.add_argument("--copernicus-user", help="SciHub username")
    cw.add_argument("--copernicus-pass", help="SciHub password")
    cw.add_argument("--non-interactive", action="store_true")
    cw.add_argument("--force", choices=("cloud", "offline"), help="Override estimator")

    return p.parse_args()

# ---------------------------------------------------------------------

def main() -> None:
    args = _cli()
    if args.command == "clearwater":
        clearwater(args)
    else:
        sys.exit("Unknown command")

# ---------------------------------------------------------------------
if __name__ == "__main__":
    main()
