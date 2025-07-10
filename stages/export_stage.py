#!/usr/bin/env python3
"""
export_stage.py
────────────────────────────────────────────────────────────
Cloud-workflow stage ❶

• Reads the AOI shapefile.
• Builds a *clear-water composite* from Sentinel-2 (or an ACOLITE asset
  if the user already ran generate_acolite.py).
• Exports the composite in ~25 km × 25 km COG tiles into the user’s
  Google-Cloud-Storage bucket.

The heavy lifting (cloud filtering, mosaicking) is done by Earth Engine;
the export tasks run server-side so your own bandwidth is only used to
push task definitions, not to download imagery.

Author : reefmap team · 2025-07-07
Licence: MIT
"""
# ──────────────────────────────────────────────────────────
import argparse, json, sys, time
from pathlib import Path

import geopandas as gpd
import ee

# ──────────────────────────────────────────────────────────
def parse_cli() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Export clear-water composite to Cloud bucket.")
    p.add_argument("--aoi", required=True, help="AOI shapefile (.shp)")
    p.add_argument("--bucket", required=True, help="Google Cloud Storage bucket")
    p.add_argument("--tile_size", type=int, default=25000,
                   help="Tile width/height in metres (default 25 km)")
    p.add_argument("--scale", type=int, default=10,
                   help="Pixel size in metres (default 10 m)")
    p.add_argument("--start_year", type=int, default=2017)
    p.add_argument("--end_year", type=int, default=2025)
    p.add_argument("--cloud_pct", type=int, default=20,
                   help="Cloudy_pixel_percentage threshold")
    p.add_argument("--use_acolite", action="store_true",
                   help="Look for an ACOLITE asset with DSF-corrected scenes")
    return p.parse_args()


# ──────────────────────────────────────────────────────────
def init_ee() -> None:
    """Authenticate & initialise Earth Engine."""
    try:
        ee.Initialize()
    except ee.EEException:
        print("First-time EE authenticate…")
        ee.Authenticate()
        ee.Initialize()


# ──────────────────────────────────────────────────────────
def load_aoi(shp_path: str) -> ee.Geometry:
    """Read shapefile, dissolve to single polygon, convert to EE geometry."""
    gdf = gpd.read_file(shp_path).to_crs("EPSG:4326")
    geojson = json.loads(gdf.unary_union.__geo_interface__)
    return ee.Geometry(geojson)


# ──────────────────────────────────────────────────────────
def build_clear_water(image_collection: ee.ImageCollection,
                      aoi: ee.Geometry,
                      cloud_pct: int) -> ee.Image:
    """
    1. Pre-filter: overall CLOUDY_PIXEL_PERCENTAGE (< cloud_pct).
    2. Pixel-level cloud mask from QA60.
    3. Median mosaic of remaining dates.
    """
    def mask_sentinel2(img):
        qa = img.select("QA60")
        cloud_bit  = 1 << 10
        cirrus_bit = 1 << 11
        mask = (qa.bitwiseAnd(cloud_bit).eq(0)) \
               .And(qa.bitwiseAnd(cirrus_bit).eq(0))
        return img.updateMask(mask)

    filtered = (
        image_collection
        .filter(ee.Filter.lt("CLOUDY_PIXEL_PERCENTAGE", cloud_pct))
        .map(mask_sentinel2)
    )
    print("Images after cloud filter:", filtered.size().getInfo())
    return filtered.median().clip(aoi)


# ──────────────────────────────────────────────────────────
def export_tiled(image: ee.Image,
                 aoi: ee.Geometry,
                 bucket: str,
                 tile_size: int,
                 scale: int) -> list[ee.batch.Task]:
    """
    Split AOI bounding box into square tiles (tile_size metres),
    start one export task per tile.
    """
    # Projected bbox in EPSG:3857
    bounds = ee.Geometry.BBox(*aoi.bounds().coordinates().getInfo()[0])
    xmin, ymin, xmax, ymax = bounds.coordinates().flatten()

    tasks = []
    x = xmin
    while x < xmax:
        y = ymin
        while y < ymax:
            tile = ee.Geometry.Rectangle(
                [x, y, min(x + tile_size, xmax), min(y + tile_size, ymax)],
                proj="EPSG:3857", geodesic=False
            )
            name = f"cw_{int(x)}_{int(y)}"
            task = ee.batch.Export.image.toCloudStorage(
                image=image,
                description=name,
                bucket=bucket,
                fileNamePrefix=f"clear_water/{name}",
                region=tile,
                scale=scale,
                crs="EPSG:3857",
                maxPixels=1e13,
                fileFormat="GeoTIFF",
                formatOptions={"cloudOptimized": True},
            )
            task.start()
            tasks.append(task)
            y += tile_size
        x += tile_size
    print(f"Started {len(tasks)} export tasks.")
    return tasks


# ──────────────────────────────────────────────────────────
def main() -> None:
    args = parse_cli()
    init_ee()

    aoi = load_aoi(args.aoi)

    # Choose collection
    if args.use_acolite:
        # Require that user pushed ACOLITE asset to their EE Assets
        coll_id = "users/your_username/acolite_s2_cw"
        s2_coll = ee.ImageCollection(coll_id).filterBounds(aoi)
        if s2_coll.size().getInfo() == 0:
            sys.exit(f"🛑  ACOLITE asset {coll_id} not found or empty.")
        print("✔  Using ACOLITE-corrected collection.")
    else:
        s2_coll = (
            ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED")
            .filterDate(f"{args.start_year}-01-01", f"{args.end_year}-12-31")
            .filterBounds(aoi)
        )
        print("✔  Using Sentinel-2 SR collection.")

    composite = build_clear_water(s2_coll, aoi, args.cloud_pct)
    tasks = export_tiled(composite, aoi, args.bucket,
                         args.tile_size, args.scale)

    # Print EE task URLs for monitoring
    for t in tasks:
        print(f"{t.id} – {t.status()['state']}")

    print("\nMonitor tasks in EE Code Editor ➜ Tasks tab. "
          "run_pipeline.py will poll and download once they finish.")

# -----------------------------------------------------------------
if __name__ == "__main__":
    main()
