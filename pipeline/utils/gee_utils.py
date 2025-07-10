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

    Returns:
        None. Initializes the ee client.
    """
    # Load from config file if provided
    if config_path:
        cfg_file = Path(config_path)
        if not cfg_file.exists():
            raise FileNotFoundError(f"Config file not found: {config_path}")
        with open(cfg_file, 'r') as f:
            cfg = json.load(f)
        service_account = cfg.get('service_account')
        key_path = cfg.get('key_path')

    # Validate inputs
    if not service_account or not key_path:
        raise ValueError(
            "Service account and key path must be provided either via args or config file."
        )
    if not os.path.exists(key_path):
        raise FileNotFoundError(f"Service account key not found: {key_path}")

    # Set environment variable for Earth Engine
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = str(key_path)

    # Authenticate and initialize
    try:
        ee.Initialize(
            service_account=service_account,
            credentials=ee.ServiceAccountCredentials(
                service_account, key_path
            )
        )
        print("Earth Engine initialized with service account:", service_account)
    except Exception as e:
        raise RuntimeError(f"Failed to initialize Earth Engine: {e}")


if __name__ == '__main__':
    # Example usage: python gee_utils.py /path/to/config.json
    import argparse

    parser = argparse.ArgumentParser(
        description="Initialize Google Earth Engine client."
    )
    parser.add_argument(
        'config',
        help="Path to JSON config file with 'service_account' and 'key_path'."
    )
    args = parser.parse_args()
    initialize_ee(config_path=args.config)
