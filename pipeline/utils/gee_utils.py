import ee
import json
import os
from pathlib import Path

def initialize_ee(config_path: str = None, service_account: str = None, key_path: str = None):
    """
    Initialize Google Earth Engine client.

    Args:
        config_path: Path to a JSON config file containing credentials:
            {
              "service_account": "your-service-account@your-project.iam.gserviceaccount.com",
              "key_path": "/path/to/your-service-account-key.json"
            }
        service_account: Optional override for service account email.
        key_path: Optional override for path to service account key JSON.

    Raises:
        FileNotFoundError: If config file or key path is missing.
        ValueError: If required credentials are not provided.
        RuntimeError: If Earth Engine initialization fails.

    Returns:
        None. Earth Engine client is initialized.
    """
    # Load credentials from config file if provided
    if config_path:
        cfg_file = Path(config_path)
        if not cfg_file.exists():
            raise FileNotFoundError(f"Config file not found: {config_path}")
        with cfg_file.open('r') as f:
            cfg = json.load(f)
        service_account = cfg.get('service_account')
        key_path = cfg.get('key_path')

    # Validate inputs
    if not service_account or not key_path:
        raise ValueError(
            "Service account and key path must be provided either via args or config file."
        )
    if not Path(key_path).exists():
        raise FileNotFoundError(f"Service account key not found: {key_path}")

    # Set up environment for authentication
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = str(key_path)

    # Authenticate and initialize the Earth Engine client
    try:
        credentials = ee.ServiceAccountCredentials(service_account, key_path)
        ee.Initialize(service_account=service_account, credentials=credentials)
        print(f"Earth Engine initialized for service account: {service_account}")
    except Exception as e:
        raise RuntimeError(f"Failed to initialize Earth Engine: {e}")


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(
        description="Initialize Google Earth Engine client using a service account."
    )
    parser.add_argument(
        'config',
        help="Path to JSON config file with 'service_account' and 'key_path'."
    )
    parser.add_argument(
        '--service_account', '-s',
        help="Override service account email."
    )
    parser.add_argument(
        '--key_path', '-k',
        help="Override path to service account key JSON."
    )
    args = parser.parse_args()
    initialize_ee(config_path=args.config,
                 service_account=args.service_account,
                 key_path=args.key_path)
