# pipeline/clearwater/offline_runner.py
"""
Run one tile through ACOLITE in  ❱ the current container  (preferred)  or
                                      ❱ a child ACOLITE Docker image.

• Inside the marine-habitat-pipeline image we already have ACOLITE checked
  out under /opt/acolite.  We invoke that directly.

• Otherwise, if the user asked for a separate ACOLITE image *and* the host
  still has the Docker CLI, we do Docker-in-Docker.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
from pathlib import Path
from typing import Dict, Optional

# --------------------------------------------------------------------- constants
ACOLITE_CLI_IN_CONTAINER = os.getenv(
    "ACOLITE_CLI",
    "/opt/acolite/acolite.py"          
)

# Entry-point used *inside* the official `acolite/acolite` image
ACOLITE_CLI_IN_CHILD = "/acolite/launch_acolite.py"

# ----------------------------------------------------------------- helpers
def _write_tile_geojson(tile_geom, tile_id: str, outdir: Path) -> Path:
    """Dump shapely geometry as GeoJSON and return the file path."""
    outdir.mkdir(parents=True, exist_ok=True)
    gj = {
        "type": "FeatureCollection",
        "features": [{
            "type": "Feature",
            "properties": {},
            "geometry": tile_geom.__geo_interface__,
        }]
    }
    aoi_path = outdir / f"{tile_id}_aoi.geojson"
    aoi_path.write_text(json.dumps(gj))
    return aoi_path


def _have_docker() -> bool:
    """Is the `docker` CLI available in PATH?"""
    return shutil.which("docker") is not None


def _run(cmd: list[str]) -> None:
    print("  $", " ".join(cmd))
    subprocess.run(cmd, check=True)


# ------------------------------------------------------------ run in container
def _run_acolite_direct(tile_geom, tile_id: str,
                        config: Dict, outdir: Path) -> Optional[str]:
    """Use the ACOLITE copy bundled in this image."""
    aoi = _write_tile_geojson(tile_geom, tile_id, outdir)
    cli = config.get("acolite_cli_path", ACOLITE_CLI_IN_CONTAINER)
    s2  = config.get("offline_s2_path", "/input/S2")

    cmd = [
        "python3", cli, "--cli",
        f"input={s2}",
        f"output={outdir}",
        f"region_file={aoi}",
        f"start_date={config['start_date']}",
        f"end_date={config['end_date']}",
    ]
    if config.get("acolite_extra_args"):
        cmd.extend(config["acolite_extra_args"].split())

    print(f"Running ACOLITE inside container for {tile_id}")
    try:
        _run(cmd)
        print(f"✅ ACOLITE finished for {tile_id}")
        return str(outdir)
    except subprocess.CalledProcessError as e:
        print(f"❌ ACOLITE failed for {tile_id}: {e}")
        return None


# ------------------------------------------------------------- run child image
def _run_acolite_child_docker(tile_geom, tile_id: str,
                              config: Dict, outdir: Path) -> Optional[str]:
    """Spin up a separate `acolite/acolite` image (Docker-in-Docker)."""
    if not _have_docker():
        print("Docker CLI not found in this container – cannot run child image.")
        return None

    aoi = _write_tile_geojson(tile_geom, tile_id, outdir)
    img = config.get("acolite_docker_img", "acolite/acolite:latest")
    s2  = os.path.abspath(config.get("offline_s2_path", "/input/S2"))

    cmd = [
        "docker", "run", "--rm",
        "-v", f"{s2}:/input:ro",
        "-v", f"{os.path.abspath(outdir)}:/output",
        "-v", f"{aoi}:/aoi.geojson:ro",
        img,
        "python3", ACOLITE_CLI_IN_CHILD, "--cli",
        "input=/input",
        "output=/output",
        "region_file=/aoi.geojson",
        f"start_date={config['start_date']}",
        f"end_date={config['end_date']}",
    ]
    if config.get("acolite_extra_args"):
        cmd.extend(config["acolite_extra_args"].split())

    print(f"Running ACOLITE child-docker for {tile_id}")
    try:
        _run(cmd)
        print(f"✅ Child ACOLITE finished for {tile_id}")
        return str(outdir)
    except subprocess.CalledProcessError as e:
        print(f"❌ Child ACOLITE failed for {tile_id}: {e}")
        return None


# -------------------------------------------------------------- public entry
def process_tile_offline(tile_geom, tile_id: str,
                         config: Dict) -> Optional[str]:
    """
    Decide strategy → run ACOLITE → return output folder (or None on failure).
    """
    outdir = Path(config.get("offline_output_dir", "outputs/offline")) / tile_id

    # Prefer bundled ACOLITE if present
    if Path(ACOLITE_CLI_IN_CONTAINER).exists():
        return _run_acolite_direct(tile_geom, tile_id, config, outdir)

    # Fallback: child image
    return _run_acolite_child_docker(tile_geom, tile_id, config, outdir)
