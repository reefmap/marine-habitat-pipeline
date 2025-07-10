import ee
import json
import os
from pathlib import Path


def initialize_ee(config_path: str = None,
                  service_account: str = None,
                  key_path: str = None,
                  project: str = None,
                  interactive: bool = False):
    """
    Initialize Google Earth Engine client with guidance on errors.

    Supports:
      1. Service Account auth via config or overrides.
      2. Personal CLI credentials stored in standard paths.

    All ee.Initialize() calls will explicitly pass the `project` override if provided.

    If `interactive` is True, prompts for missing credentials or project ID.

    Args:
        config_path: Path to JSON config with 'service_account', 'key_path', and optional 'project'.
        service_account: Service account email.
        key_path: Path to service account key JSON.
        project: GCP project ID for Earth Engine (can set env var EARTHENGINE_PROJECT/GEE_PROJECT).
        interactive: Prompt interactively if credentials/project missing.

    Raises:
        FileNotFoundError: If required credentials files are missing.
        ValueError: If config inputs are incomplete.
        RuntimeError: For EE API errors or unregistered project.
    """
    # Determine project from args or environment
    project = project or os.environ.get('EARTHENGINE_PROJECT') or os.environ.get('GEE_PROJECT')

    def try_initialize(creds=None):
        """
        Always call ee.Initialize with the explicit project override.
        If creds is None, we rely on personal CLI/ADC credentials.
        """
        try:
            if creds:
                ee.Initialize(credentials=creds, project=project)
            else:
                ee.Initialize(project=project)
            return True
        except ee.EEException as e:
            msg = str(e)
            if 'Not signed up' in msg or 'project is not registered' in msg:
                raise RuntimeError(
                    "Earth Engine project/account not registered. "
                    "Visit https://developers.google.com/earth-engine/guides/access to register."
                )
            return False

    # 1) Service-account flow
    if config_path or service_account or key_path:
        # Load config JSON if provided
        if config_path:
            cfg = json.loads(Path(config_path).read_text())
            service_account = service_account or cfg.get('service_account')
            key_path = key_path or cfg.get('key_path')
            project = project or cfg.get('project')
        if not service_account or not key_path:
            raise ValueError("Service account and key_path must be provided for service-account auth.")
        key_file = Path(key_path)
        if not key_file.exists():
            raise FileNotFoundError(f"Service account key not found: {key_path}")
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = str(key_file)
        creds = ee.ServiceAccountCredentials(service_account, str(key_file))
        if try_initialize(creds=creds):
            print(f"Authenticated with service account {service_account} (project: {project})")
            return
        if interactive:
            print("Service-account auth failed. Please verify your service account, project ID, and permissions.")
        raise RuntimeError("Earth Engine service-account initialization failed.")

    # 2) Personal CLI credentials flow
    cred_paths = []
    if os.environ.get('EARTHENGINE_CREDENTIALS'):
        cred_paths.append(Path(os.environ['EARTHENGINE_CREDENTIALS']))
    cred_paths.append(Path.cwd() / '.config' / 'earthengine' / 'credentials')
    cred_paths.append(Path.home() / '.config' / 'earthengine' / 'credentials')
    if os.name == 'nt':
        cred_paths.append(Path(os.environ.get('APPDATA', '')) / 'earthengine' / 'credentials')

    for cred in cred_paths:
        if cred.exists():
            if try_initialize(creds=None):
                print(f"Initialized with CLI credentials from {cred} (project: {project})")
                return
            break

    # 3) Interactive fallback
    if interactive:
        print("No valid Earth Engine credentials found or project unauthorized.")
        print("1. If you don't have an EE account, sign up at https://developers.google.com/earth-engine/guides/access")
        cred_input = input("Enter path to credentials file (or leave blank to skip): ").strip()
        if cred_input:
            os.environ['EARTHENGINE_CREDENTIALS'] = cred_input
            if try_initialize(creds=None):
                print(f"Initialized with provided credentials at {cred_input} (project: {project})")
                return
        proj_input = input("Enter your registered Earth Engine GCP project ID: ").strip()
        if proj_input:
            project = proj_input
            if try_initialize(creds=None):
                print(f"Initialized with project override {project}")
                return
        raise RuntimeError("Could not initialize Earth Engine. Please check your credentials and project registration.")

    # 4) Non-interactive error guidance
    raise FileNotFoundError(
        "Earth Engine initialization failed.\n"
        "- Ensure you have credentials in ~/.config/earthengine/credentials or set EARTHENGINE_CREDENTIALS.\n"
        "- If using a service account, provide --config JSON with service_account/key_path/project.\n"
        "- To sign up or register your project, visit: https://developers.google.com/earth-engine/guides/access"
    )


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description="Initialize Earth Engine client.")
    parser.add_argument('-c','--config', help="JSON config with service_account/key_path/project.")
    parser.add_argument('-s','--service_account', help="Service account email.")
    parser.add_argument('-k','--key_path', help="Path to service account key.")
    parser.add_argument('-p','--project', help="GCP project ID for Earth Engine.")
    parser.add_argument('-i','--interactive', action='store_true', help="Prompt for missing info.")
    args = parser.parse_args()
    initialize_ee(
        config_path=args.config,
        service_account=args.service_account,
        key_path=args.key_path,
        project=args.project,
        interactive=args.interactive
    )
