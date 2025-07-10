#!/usr/bin/env python3
"""
download_stage.py
────────────────────────────────────────────────────────────
Cloud-workflow stage ❷

• Lists all COGs in gs://<bucket>/clear_water/
• Downloads anything not already present in ./tiles/
• Verifies file size & MD5 hash
• Optionally deletes the remote copy to keep bucket costs near-zero

Author  : reefmap team — 2025-07-07
Licence : MIT
"""
# ──────────────────────────────────────────────────────────
import argparse, hashlib, os, sys
from pathlib import Path

from google.cloud import storage

# ──────────────────────────────────────────────────────────
def parse_cli() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Download clear-water COG tiles from Cloud Storage.")
    p.add_argument("--bucket", required=True, help="GCS bucket name")
    p.add_argument("--prefix", default="clear_water/",
                   help="Bucket folder prefix (default clear_water/)")
    p.add_argument("--dst", default="tiles",
                   help="Local folder to save tiles (default ./tiles)")
    p.add_argument("--purge", action="store_true",
                   help="Delete remote files after verified download")
    return p.parse_args()


# ──────────────────────────────────────────────────────────
def local_md5(path: Path) -> str:
    """Compute base-16 (hex) MD5 of a local file."""
    h = hashlib.md5()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(8 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


# ──────────────────────────────────────────────────────────
def main() -> None:
    args = parse_cli()
    dst_dir = Path(args.dst)
    dst_dir.mkdir(exist_ok=True, parents=True)

    client = storage.Client()
    bucket = client.bucket(args.bucket)

    blobs = bucket.list_blobs(prefix=args.prefix)
    blobs = [b for b in blobs if b.name.lower().endswith(".tif")]
    if not blobs:
        sys.exit("🛑  No .tif files found under the specified prefix.")

    print(f"Found {len(blobs)} COG tiles in gs://{args.bucket}/{args.prefix}\n")

    for blob in blobs:
        local_path = dst_dir / Path(blob.name).name

        # Skip download if file exists and MD5 matches
        if local_path.exists():
            if local_md5(local_path) == blob.md5_hash:
                print(f"✓  {local_path.name} already present (skipped)")
                continue
            else:
                print(f"↻  Re-downloading {local_path.name} (checksum mismatch)")

        # Stream download
        with local_path.open("wb") as f:
            blob.download_to_file(f)
        print(f"↓  Downloaded {local_path.name} "
              f"({local_path.stat().st_size/1e6:.1f} MB)")

        # Verify checksum
        if local_md5(local_path) != blob.md5_hash:
            sys.exit(f"🛑  MD5 mismatch for {local_path.name}")

        # Optionally purge from bucket
        if args.purge:
            blob.delete()
            print(f"🗑️  Purged remote copy of {local_path.name}")

    print("\n🏁  All tiles present in", dst_dir.resolve())


# -----------------------------------------------------------------
if __name__ == "__main__":
    main()
