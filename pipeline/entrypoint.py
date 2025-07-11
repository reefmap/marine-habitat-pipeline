# pipeline/entrypoint.py
# ---------------------------------------------------------------------
"""
Unified CLI for the marine-habitat pipeline.

$ python -m pipeline.entrypoint clearwater \
        --aoi reef.geojson \
        --start 2024-06-01 --end 2024-06-30 \
        --out s3://reefmap/mosaics/2024/ \
        --gee-service-account gee-sa.json \
        --copernicus-user you@example.com --copernicus-pass **** \
        --non-interactive
"""
# ---------------------------------------------------------------------
from __future__ import annotations
import argparse, json, os, sys, tempfile, textwrap, pathlib
import datetime as dt
import urllib.parse
import geopandas as gpd
import shapely
import ee
from .common.autodata import ensure_tidal_asset
from .clearwater import tiler, cloud_runner, offline_runner, estimate
from .clearwater.s2_fetch import fetch_scenes

# ---------------------------------------------------------------------
DEF_CFG = {
    "cloud_thresh"            : 20,
    "chla_thresh"             : 0.3,
    "wind_thresh"             : 4.5,
    "tidal_thresh_m"          : 0.5,
    "water_occurrence_thresh" : 80,
    "max_scenes"              : 50,
    "gee_scale"               : 10,
    # cost model for estimator  --------------------
    "scene_size_gb"           : 0.55,
    "cpu_hours_per_tile"      : 0.12,
    "storage_cost_gb"         : 0.023,   # $/GB-month
    "compute_cost_hr"         : 0.048,   # $/vCPU-h
}

# ---------------------------------------------------------------------
def _load_aoi(aoi_str: str) -> gpd.GeoDataFrame:
    """Accept path, URL or raw GeoJSON string and return a GeoDataFrame."""
    if aoi_str.strip().startswith("{"):
        return gpd.GeoDataFrame.from_features(json.loads(aoi_str))
    if urllib.parse.urlparse(aoi_str).scheme in ("http", "https", "ftp"):
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".geojson")
        pathlib.Path(tmp.name).write_bytes(
            urllib.request.urlopen(aoi_str).read()
        )
        return gpd.read_file(tmp.name)
    return gpd.read_file(aoi_str)

# ---------------------------------------------------------------------
def _decide_mode(tile_stats: list[dict], cfg: dict, force: str | None):
    """Return 'cloud' or 'offline'."""
    if force:
        return force
    est = estimate.estimate_resources(
        tile_stats,
        avg_scene_size_gb = cfg["scene_size_gb"],
        cpu_hours_per_tile= cfg["cpu_hours_per_tile"],
        storage_rate_per_gb= cfg["storage_cost_gb"],
        cpu_rate_per_hour = cfg["compute_cost_hr"],
    )
    # naive heuristic: if > 50 GB storage or > 4 CPU-h  ➜ cloud
    return "cloud" if est["storage_gb"] > 50 or est["cpu_hours"] > 4 else "offline"

# ---------------------------------------------------------------------
def clearwater(args: argparse.Namespace):
    # ----------------  EE login  ----------------
    if args.gee_service_account:
        ee.Initialize(
            ee.ServiceAccountCredentials(
                email=args.gee_service_account,
                key_file=args.gee_service_account,
            )
        )
    else:
        ee.Initialize()      # uses saved user creds

    ensure_tidal_asset()     # ← upload/download once, then cache

    # ----------------  AOI & tiling  -------------
    aoi = _load_aoi(args.aoi)
    tiles = tiler.split_aoi(aoi, km=1)      # returns list[gdf rows]

    cfg_path = pathlib.Path(args.config or "config.json")
    cfg      = DEF_CFG.copy()
    if cfg_path.exists():
        cfg.update(json.loads(cfg_path.read_text()))
    cfg.update({
        "start_date": args.start,
        "end_date"  : args.end,
        "gee_bucket": args.out.split("/", 3)[2] if args.out.startswith("s3://") else args.out,
        "gee_folder": "/".join(args.out.split("/", 3)[3:]) if args.out.startswith("s3://") else "",
    })
    cfg_path.write_text(json.dumps(cfg, indent=2))

    # ----------------  Resource estimate --------
    tile_stats = [{"n_scenes": cfg["max_scenes"]} for _ in tiles]  # quick proxy
    mode = _decide_mode(tile_stats, cfg, args.force)
    print(f"▶ Running Lane-1 in **{mode.upper()}** mode")

    # ----------------  Loop tiles  --------------
    task_ids = []
    for idx, tile in enumerate(tiles):
        geom = shapely.geometry.shape(tile["geometry"].__geo_interface__)
        tid  = f"{tile['id']}_{idx:02}"
        if mode == "cloud":
            task_id = cloud_runner.process_tile_cloud(geom, tid, cfg)
        else:
            fetch_scenes(
                aoi_wkt = geom.wkt,
                start   = cfg["start_date"],
                end     = cfg["end_date"],
                user    = args.copernicus_user,
                pwd     = args.copernicus_pass,
                outdir  = pathlib.Path(args.out) / "scenes",
            )
            task_id = offline_runner.process_tile_offline(geom, tid, cfg)
        task_ids.append(task_id)

    print("Tiles kicked off:", task_ids)

# ---------------------------------------------------------------------
def _cli() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="mhp",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=textwrap.dedent("""\
            Marine-Habitat Pipeline

            Examples
            --------
            * Non-interactive one-liner (runs estimator & autopicks cloud/offline)
              $ mhp clearwater --aoi reef.geojson --start 2024-06-01 --end 2024-06-30 --out /tmp/mosaic --non-interactive

            * Force cloud with S3 export
              $ mhp clearwater --aoi https://.../reef.geojson \\
                               --start 2023-01-01 --end 2023-12-31 \\
                               --out s3://reefmap/mosaic/2023/ --force cloud
        """)
    )
    sub = p.add_subparsers(dest="command", required=True)

    cw = sub.add_parser("clearwater", help="Lane-1 clear-water mosaic")
    cw.add_argument("--aoi", required=True,
                    help="File path, URL or raw GeoJSON string")
    cw.add_argument("--start", required=True, type=str)
    cw.add_argument("--end",   required=True, type=str)
    cw.add_argument("--out",   required=True,
                    help="Local folder OR s3://bucket/prefix")
    cw.add_argument("--config", help="Existing config.json to load / overwrite")
    cw.add_argument("--gee-service-account",
                    help="Path to GEE service-account JSON key")
    cw.add_argument("--copernicus-user", help="SciHub username")
    cw.add_argument("--copernicus-pass", help="SciHub password")
    cw.add_argument("--non-interactive", action="store_true",
                    help="Fail if required args are missing instead of asking")
    cw.add_argument("--force", choices=("cloud", "offline"),
                    help="Bypass estimator and choose path directly")

    return p.parse_args()

# ---------------------------------------------------------------------
def main():
    args = _cli()
    if args.command == "clearwater":
        clearwater(args)
    else:
        sys.exit("Unknown command")

# ---------------------------------------------------------------------
if __name__ == "__main__":
    main()
