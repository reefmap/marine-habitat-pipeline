"""
terrain.py

Derives slope and BPI from a DEM.
"""

import rasterio
import numpy as np

def compute_slope(dem_path, config):
    """
    Compute terrain slope from DEM.
    """
    # TODO: Read DEM, calculate gradient
    raise NotImplementedError("compute_slope not implemented")

def compute_bpi(dem_path, config):
    """
    Compute topographic position index (BPI) from DEM.
    """
    # TODO: Implement BPI calculation
    raise NotImplementedError("compute_bpi not implemented")
