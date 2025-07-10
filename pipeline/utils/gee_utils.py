import ee
import json
import os
from pathlib import Path

def initialize_ee(config_path: str = None, service_account: str = None, key_path: str = None):
    """
    Initialize Google Earth Engine client.

    Supports two auth modes:
      1. Service Account: via config file or overrides.
      2. User Credentials: auto-detect CLI credentials in standard or project-local paths.

    Args:
        config_path: Path to JSON config with 'service_account' and 'key_path'.
        service_account: Service account email override.
        key_path: Service account key JSON override.

    Raises:
        FileNotFoundError: If required files are missing.
        ValueError: If provided inputs are incomplete.
        RuntimeError: If auth/init fails.

    Returns:
        None. Earth Engine client initialized.
    """
    # 1) If no service-account inputs, try user credentials
    if not any([config_path, service_account, key_path]):
        # Collect possible credential paths
        default_paths = []
        # Env var override first
        if os.environ.get('EARTHENGINE_CREDENTIALS'):
            default_paths.append(Path(os.environ['EARTHENGINE_CREDENTIALS']))
        # Project-local credentials
        default_paths.append(Path.cwd() / '.config' / 'earthengine' / 'credentials')
        # XDG on Unix/Mac
        default_paths.append(Path.home() / '.config' / 'earthengine' / 'credentials')
        # APPDATA on Windows
        if os.name == 'nt':
            default_paths.append(Path(os.environ.get('APPDATA', '')) / 'earthengine' / 'credentials')

        # Try each path for existing credentials
        for cred in default_paths:
            if cred and cred.exists():
                try:
                    ee.Initialize()
                    print(f"Earth Engine initialized using credentials from {cred}")
                    return
                except Exception:
                    # If init fails (e.g., not registered), capture but continue to next
                    continue
        # No valid user credentials found
        raise RuntimeError(
            "Earth Engine credentials not found or invalid.
"
            "Please mount your Earth Engine CLI credentials at one of the following locations inside the container:
"
            f"  - $EARTHENGINE_CREDENTIALS (env var)
"
            f"  - {Path.cwd() / '.config' / 'earthengine' / 'credentials'}
"
            f"  - {Path.home() / '.config' / 'earthengine' / 'credentials'}
"
            "Ensure your user or service account is registered for Earth Engine (see https://developers.google.com/earth-engine/guides/access)."
        )

# 2) Load service-account creds if provided via config file
    if config_path:
        cfg_file = Path(config_path)
        if not cfg_file.exists():
            raise FileNotFoundError(f"Config file not found: {config_path}")
        with cfg_file.open('r') as f:
            cfg = json.load(f)
        service_account = cfg.get('service_account')
        key_path = cfg.get('key_path')

    # 3) Validate service-account inputs
    if not service_account or not key_path:
        raise ValueError("Service account and key path must be provided via config or args.")
    key_file = Path(key_path)
    if not key_file.exists():
        raise FileNotFoundError(f"Service account key not found: {key_path}")

    # 4) Set env var for Google creds
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = str(key_file)

    # 5) Authenticate via service account
    try:
        credentials = ee.ServiceAccountCredentials(service_account, str(key_file))
        ee.Initialize(service_account=service_account, credentials=credentials)
        print(f"Earth Engine initialized for service account: {service_account}")
    except Exception as e:
        raise RuntimeError(f"Service account auth/init failed: {e}")


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(
        description="Initialize Google Earth Engine client."
    )
    parser.add_argument('-c', '--config', help="Path to JSON config with 'service_account' and 'key_path'.")
    parser.add_argument('-s', '--service_account', help="Service account email override.")
    parser.add_argument('-k', '--key_path', help="Service account key JSON override.")
    args = parser.parse_args()
    initialize_ee(config_path=args.config,
                 service_account=args.service_account,
                 key_path=args.key_path)
