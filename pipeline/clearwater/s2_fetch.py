"""
s2_fetch.py
-----------

Temporary stub so `entrypoint.py` can import `pipeline.clearwater.s2_fetch`.

When the pipeline decides to run **offline**, this function is supposed to
download all Sentinel-2 L1C scenes that intersect the AOI and date range,
using the Copernicus Open Access Hub API (e.g. via `sentinelsat`).

For now it just prints what it *would* do and returns a dummy task-id string.
"""

from pathlib import Path
from datetime import datetime

def fetch_scenes(*, aoi_wkt: str, start: str, end: str,
                 user: str, pwd: str, outdir: Path) -> str:
    """Stub – log parameters and pretend the download is done."""
    print("⚠ [s2_fetch] stub called")
    print(f"    AOI WKT  : {aoi_wkt[:60]}…")
    print(f"    Date     : {start} → {end}")
    print(f"    User     : {user!r}")
    print(f"    Out dir  : {outdir}")
    outdir.mkdir(parents=True, exist_ok=True)
    # touch a sentinel file so downstream code can see something
    (outdir / "DOWNLOAD_COMPLETE.flag").touch()
    # return a fake “task id” so the caller can log it
    return f"stub-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"
