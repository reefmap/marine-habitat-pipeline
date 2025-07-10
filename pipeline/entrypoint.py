from .config import load_config
from .clearwater.tiler import load_aoi, buffer_aoi

def main():
    print("Welcome to the Marine Habitat Pipeline!")
    config = load_config()
    aoi = load_aoi(config["aoi_path"])
    # Buffer if needed (simple logic for demo)
    if config.get("buffer_aoi", False):
        aoi = buffer_aoi(aoi, buffer_km=config.get("buffer_km", 2))
    # (Next: pass AOI to filter/scene logic)
