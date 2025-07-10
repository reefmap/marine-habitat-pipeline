#!/usr/bin/env python3
"""
utils.py
────────────────────────────────────────────────────────────
Small helper library shared by multiple stage scripts.

Contents
--------
1. CLI helpers  – ask(), ask_choice(), ask_yesno()
2. Quick COG exporter – export_raster()
3. ERA5-Wave downloader – download_era5_wave()

No heavy external dependencies beyond what the Dockerfile
already installs.

2025-07-07 | reefmap team | MIT Licence
"""
# ───────────────────────── Imports ────────────────────────
from pathlib import Path
import json
import rasterio
from rasterio.enums import Resampling

# ------------------------------------------------------------------
# 1. CLI helpers
# ------------------------------------------------------------------
def ask(prompt: str, default: str = "") -> str:
    """Simple input with default fallback (handles non-interactive shell)."""
    try:
        return input(f"{prompt} ").strip() or default
    except EOFError:                # non-interactive Docker run
        return default


def ask_choice(prompt: str, choices: list[str], default: str | None = None) -> str:
    """Repeat until the user types one of the allowed options."""
    choice_str = "/".join(choices)
    while True:
        reply = ask(f"{prompt} ({choice_str})", default or "")
        if reply in choices:
            return reply
        print("Please choose:", ", ".join(choices))


def ask_yesno(prompt: str, default: str = "n") -> bool:
    """Return True for 'y', False for 'n'."""
    return ask_choice(prompt, ["y", "n"], default) == "y"


# ------------------------------------------------------------------
# 2. Quick Cloud-Optimised GeoTIFF exporter   (Earth Engine ➜ COG)
# ------------------------------------------------------------------
def export_raster(
    dst_path: Path | str,
    ee_image,
    region,
    scale: int,
    crs: str = "EPSG:3857",
    nodata: float = float("nan"),
) -> None:
    """
    Download a **small or medium** EE image (≤ ~200 MB) to a local
    Cloud-Optimised GeoTIFF using geemap.ee_to_numpy().

    Parameters
    ----------
    dst_path : Path | str   – output filename
    ee_image : ee.Image     – Earth-Engine image
    region   : ee.Geometry  – export footprint (AOI)
    scale    : int          – pixel size in metres
    crs      : str          – target projection
    nodata   : float        – NoData value written to file
    """
    import geemap
    import numpy as np

    arr = geemap.ee_to_numpy(ee_image, region=region, scale=scale)
    if arr is None:
        raise RuntimeError("ee_to_numpy returned None – export failed.")

    # ee_to_numpy shape = [bands, rows, cols] – we assume one band
    band_arr = arr[0].astype("float32")
    rows, cols = band_arr.shape

    # Bounds helper from geemap
    xmin, ymin, xmax, ymax = geemap.utils._region_bounds(region)
    transform = rasterio.transform.from_bounds(xmin, ymin, xmax, ymax, cols, rows)

    profile = dict(
        driver="COG",
        dtype="float32",
        count=1,
        height=rows,
        width=cols,
        transform=transform,
        crs=crs,
        nodata=nodata,
        compress="DEFLATE",
    )

    dst_path = Path(dst_path)
    dst_path.parent.mkdir(parents=True, exist_ok=True)

    with rasterio.open(dst_path, "w", **profile) as dst:
        dst.write(band_arr, 1)

    size_mb = dst_path.stat().st_size / 1e6
    print(f"✓  Exported {dst_path.name}  ({size_mb:.1f} MB)")


# ------------------------------------------------------------------
# 3. ERA5-Wave subset downloader
# ------------------------------------------------------------------
def download_era5_wave(bbox, year: int, out_nc: Path | str) -> None:
    """
    Download ERA5-Wave (significant wave height, mean wave period & direction)
    for a bounding box and calendar year, saving as NetCDF.

    bbox   = [xmin, ymin, xmax, ymax]  in lat-lon degrees.
    year   = 4-digit year as int
    out_nc = output filename (.nc)

    Requires the `cdsapi` package and a ~/.cdsapirc login file.
    """
    import cdsapi

    xmin, ymin, xmax, ymax = bbox
    # CDS expects area in N/W/S/E order
    area = [ymax, xmin, ymin, xmax]     # N, W, S, E

    c = cdsapi.Client()
    c.retrieve(
        "reanalysis-era5-single-levels",
        {
            "product_type": "reanalysis",
            "variable": [
                "significant_height_of_combined_wind_waves",
                "mean_wave_period",
                "mean_wave_direction",
            ],
            "year": str(year),
            "month": [f"{m:02d}" for m in range(1, 13)],
            "day":   [f"{d:02d}" for d in range(1, 32)],
            "time":  "12:00",
            "area":  area,
            "format": "netcdf",
        },
        str(out_nc),
    )
    print(f"✓  ERA5-Wave subset ({year}) saved → {out_nc}")


# ------------------------------------------------------------------
if __name__ == "__main__":     # mini smoke-test
    colour = ask_choice("Pick a colour", ["red", "green", "blue"], "green")
    print("You picked:", colour)
