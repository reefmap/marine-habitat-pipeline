"""
stacking.py

Stacks multiple raster layers into a single multi-band image.
"""

import rasterio
from rasterio.merge import merge

def stack_layers(layer_paths, config):
    """
    Stack list of single-band rasters into a multi-band raster.
    Args:
        layer_paths: List of file paths to rasters.
        config: Pipeline configuration dict.
    Returns:
        Path to stacked multi-band raster.
    """
    # TODO: Use rasterio.open and write multi-band COG
    raise NotImplementedError("stack_layers not implemented")
