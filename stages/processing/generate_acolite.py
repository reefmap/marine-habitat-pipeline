#!/usr/bin/env python3
"""
generate_acolite.py
────────────────────────────────────────────────────────────
Local-only workflow stage ❶

1. Uses `sentinelsat` to query Copernicus SciHub for Sentinel-2 L1C
   scenes intersecting the AOI, already filtered by CLOUDY_PIXEL_PERCENTAGE.
2. Downloads each ZIP if not already cached in ./raw_scenes/.
3. Creates a tiny ACOLITE batch configuration file and calls
   `acolite_cli.py` in DSF mode to atmospherically correct every ZIP.
4. Converts the resulting 16-bit reflectance bands to Cloud-Optimised
   GeoTIFFs (COGs) and stores them in ./tiles/.

Assumptions
-----------
• ACOLITE command-line is already installed in the Docker image
  (`pip install acolite` during docker build).
• SciHub credentials are provided via environment variables
  `S2_USER` and `S2_PASS` (safer than hard-coding).

Author : reefmap team — 2025-07-07
Licence: MIT
"""
# ──────────────────────────────────────────────────────────
import argparse, os, subprocess, sys, time
from pathlib import Path

import geopandas as gpd
from sentinelsat import SentinelAPI, read_geojson, geojson_to_wkt

# -----------------------------------------------------------------
def parse_cli() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Download S-2 scenes and run ACOLITE-DSF correction.")
    p.add_argument("--aoi", required=True, help="AOI shapefile (.shp)")
    p.add_argument("--output", default="tiles", help="Destination folder (default ./tiles/)")
    p.add_argument("--cloud_pct", type=int, default=20,
                   help="Max CLOUDY_PIXEL_PERCENTAGE (default 20)")
    p.add_argument("--max_scenes", type=int, default=20,
                   help="Max scenes per tile (keeps clearest N)")
    p.add_argument("--years", type=int, default=8,
                   help="Number of years back from today to search (default 8)")
    return p.parse_args()

# -----------------------------------------------------------------
def authenticate_scihub() -> SentinelAPI:
    user = os.getenv("S2_USER")
    pwd  = os.getenv("S2_PASS")
    if not user or not pwd:
        sys.exit("🛑  Set S2_USER and S2_PASS env vars for SciHub credentials.")
    return SentinelAPI(user, pwd, "https://apihub.copernicus.eu/apihub")

# -----------------------------------------------------------------
def search_scenes(api: SentinelAPI, aoi_wkt: str,
                  cloud_pct: int, years: int) -> dict:
    """Return ordered dict of SciHub products filtered by cloud %."""
    from datetime import datetime, timedelta
    start = (datetime.utcnow() - timedelta(days=365*years)).strftime("%Y-%m-%d")
    products = api.query(
        footprint=aoi_wkt,
        date=(start, "NOW"),
        platformname="Sentinel-2",
        processinglevel="Level-1C",
        cloudcoverpercentage=(0, cloud_pct))
    # Sort by ascending cloud %
    return api.to_dataframe(products).sort_values("cloudcoverpercentage")

# -----------------------------------------------------------------
def run_acolite(zip_paths: list[Path], output_dir: Path) -> None:
    """
    Build a temporary ACOLITE batch text file and run DSF correction.
    Each entry: /full/path/scene.zip
    """
    batch_txt = output_dir / "acolite_batch.txt"
    with batch_txt.open("w") as f:
        for zp in zip_paths:
            f.write(str(zp.resolve()) + "\n")

    cmd = [
        "acolite_cli.py",
        "--cli",
        f"--input={batch_txt}",
        f"--output={output_dir}",
        "--dsf",
        "--s2_l2c_level=TOA"      # ensures correct naming
    ]
    print("\n▶  Running ACOLITE DSF …")
    subprocess.check_call(cmd)
    print("✔  ACOLITE finished.\n")

# -----------------------------------------------------------------
def main() -> None:
    args = parse_cli()
    aoi_path = Path(args.aoi)
    if not aoi_path.is_file():
        sys.exit("🛑  AOI shapefile not found.")

    output_dir = Path(args.output)
    raw_dir    = Path("raw_scenes")
    output_dir.mkdir(parents=True, exist_ok=True)
    raw_dir.mkdir(parents=True, exist_ok=True)

    # ---------- 1. Authenticate & search -------------
    api = authenticate_scihub()

    # Build a quick GeoJSON + WKT for search
    gdf = gpd.read_file(aoi_path)
    aoi_wkt = geojson_to_wkt(read_geojson(gdf.to_json()))
    df = search_scenes(api, aoi_wkt, args.cloud_pct, args.years)

    if df.empty:
        sys.exit("🛑  No Sentinel-2 scenes match cloud filter.")

    # Keep only clearest N scenes
    df = df.head(args.max_scenes)
    print(f"Will download & process {len(df)} scenes.\n")

    # ---------- 2. Download scenes -------------
    downloaded_zips = []
    for uuid, row in df.iterrows():
        zip_path = raw_dir / f"{row['title']}.zip"
        if zip_path.exists():
            print(f"✓  {zip_path.name} already downloaded.")
        else:
            print(f"↓  Downloading {zip_path.name} …")
            api.download(uuid, directory_path=raw_dir)
        downloaded_zips.append(zip_path)

    # ---------- 3. Run ACOLITE DSF -------------
    run_acolite(downloaded_zips, output_dir)

    # ---------- 4. Convert reflectance bands to single COG (blue/green/NIR) -------------
    import rasterio
    from rasterio.shutil import copy as rio_copy
    from rasterio.enums import Resampling

    for dsf_dir in output_dir.glob("S2*DSF"):
        # ACOLITE places bands as *_rhot_*.tif.  We need B2,B3,B4,B8
        band_files = sorted(dsf_dir.glob("*rhot_???.tif"))
        if not band_files:
            print("⚠  No reflectance bands found in", dsf_dir.name)
            continue
        # Merge into a single COG
        srcs = [rasterio.open(str(b)) for b in band_files]
        meta = srcs[0].meta.copy()
        meta.update(driver="COG",
                    dtype="float32",
                    count=len(srcs),
                    compress="DEFLATE",
                    nodata=0)
        cog_path = output_dir / f"{dsf_dir.name}_rhot.cog.tif"
        with rasterio.open(cog_path, "w", **meta) as dst:
            for idx, src in enumerate(srcs, start=1):
                dst.write(src.read(1).astype("float32"), idx)
        print(f"✓  Wrote {cog_path.name}")

    print("\n🏁  ACOLITE-clear scenes are in", output_dir.resolve())

# -----------------------------------------------------------------
if __name__ == "__main__":
    main()
