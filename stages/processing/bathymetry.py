"""
bathymetry.py

Computes bathymetry and merges with DEM.
"""

import ee

def compute_bathymetry(dsf_path, gebco_asset, config):
    """
    Estimate bathymetry using DSF and GEBCO reference.
    """
    # TODO: Implement DSF-based bathymetry regression
    raise NotImplementedError("compute_bathymetry not implemented")

def compute_topobathy(dsf_path, bathy, config):
    """
    Merge bathymetry with GEBCO DEM into topo-bathymetry.
    """
    # TODO: Combine bathy and EE DEM
    raise NotImplementedError("compute_topobathy not implemented")
