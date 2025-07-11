import os, requests, zipfile, tempfile, ee
from pathlib import Path

CACHE = Path(os.getenv("MHP_CACHE_DIR", "~/.mhp_cache")).expanduser()
SRC_ZIP = "https://api.researchdata.se/dataset/ecds0243-1/1.0/file/zip"

# ------------------------------------------------------------------#
# Helpers                                                           #
# ------------------------------------------------------------------#
def _asset_id() -> str:
    """Compute the tidal-range asset path *after* EE is initialized."""
    root = ee.data.getAssetRoots()[0]["id"]            # users/<name>
    user = root.split("/")[1]
    return f"users/{user}/global_tidal_range"

def ensure_tidal_asset():
    """Download + upload the 5-km tidal-range raster exactly once."""
    asset = _asset_id()
    try:
        return ee.Image(asset)
    except Exception:
        pass                     # not found → fall through and ingest

    CACHE.mkdir(parents=True, exist_ok=True)
    z = CACHE / "tidal_range.zip"
    if not z.exists():
        print("▶ Downloading global tidal-range raster … (~115 MB)")
        with requests.get(SRC_ZIP, stream=True, timeout=3600) as r:
            r.raise_for_status()
            with open(z, "wb") as f:
                for chunk in r.iter_content(2 << 20):
                    f.write(chunk)

    with tempfile.TemporaryDirectory() as td:
        zipfile.ZipFile(z).extract("annual_max_cycle_amp_cm.tif", td)
        tif = os.path.join(td, "annual_max_cycle_amp_cm.tif")
        task_id = ee.data.newTaskId()[0]
        task = ee.data.startIngestion(task_id, {
            "id": asset,
            "tilesets": [{"sources": [{"uris": [tif]}]}],
        })
        ee.batch.Task(task).start()
        ee.batch.Task(task).status()['state']        # blocks
    return ee.Image(asset)
