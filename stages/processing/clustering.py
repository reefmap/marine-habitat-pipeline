"""
clustering.py

Performs k-means clustering on stacked layers.
"""

from sklearn.cluster import KMeans
import rasterio
import numpy as np

def run_kmeans(stack_path, n_clusters, config):
    """
    Apply KMeans clustering to a multi-band stack.
    Args:
        stack_path: Path to stacked multi-band raster.
        n_clusters: Number of clusters.
        config: Pipeline configuration dict.
    Returns:
        Path to clustered output raster.
    """
    with rasterio.open(stack_path) as src:
        stack = src.read()
        bands, rows, cols = stack.shape
        samples = stack.reshape(bands, -1).T
    km = KMeans(n_clusters=n_clusters, random_state=0, n_init="auto")
    labels = km.fit_predict(samples).astype(np.uint8)
    clustered = labels.reshape(rows, cols)
    out_path = config.get("output_dir", "./outputs") + "/clusters.tif"
    meta = src.meta.copy()
    meta.update({"count": 1, "dtype": "uint8"})
    with rasterio.open(out_path, "w", **meta) as dst:
        dst.write(clustered, 1)
    return out_path
