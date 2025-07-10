import json

def load_config(config_path="data/config.example.json"):
    with open(config_path) as f:
        config = json.load(f)
    return config