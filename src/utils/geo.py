import rasterio
from shapely.geometry import shape
import geopandas as gpd
import xarray as xr
import numpy as np
from rasterio.sample import sample_gen
from utils.cache import memoize_geospatial_with_persistence

import pyproj
from shapely.geometry import Point
from shapely.ops import transform

import copy
import logging



# @memoize_geospatial_with_persistence('/tmp/extract_points.pkl')
def extract_z_values(ds, gdf, column_name, offset_column=None, offset_units=None) -> gpd.GeoDataFrame:
    # note the extra 'z' dimension that our results will be organized along
    da_x = xr.DataArray(gdf.geometry.x.values, dims=['z'])
    da_y = xr.DataArray(gdf.geometry.y.values, dims=['z'])
    logging.info(da_x)
    results = ds.sel(x=da_x, y=da_y, method='nearest')
    gdf[column_name] = results.values
    gdf[column_name][gdf[column_name] == ds.rio.nodata] = 0
    gdf[column_name][gdf[column_name].isna()] = 0
    if offset_units == "ft":
        offset = gdf[offset_column] * 0.3048
        gdf[column_name] = gdf[column_name] - offset
    if offset_units == "m":
        offset = gdf[offset_column]
        gdf[column_name] = gdf[column_name] - offset
    return gdf

# Convert GeoJSON to GeoDataFrame
def geojson_to_geodataframe(geojson):
    features = geojson["features"]
    geometries = [shape(feature["geometry"]) for feature in features]
    properties = [feature["properties"] for feature in features]
    gdf = gpd.GeoDataFrame(properties, geometry=geometries)
    return gdf


def transform_point(x, y, crs):
    pt = Point(x, y)
    init_crs = pyproj.CRS(crs)
    wgs84 = pyproj.CRS('EPSG:4326')
    project = pyproj.Transformer.from_crs(init_crs, wgs84, always_xy=True).transform
    logging.info(project)
    return transform(project, pt)


def rescale_raster(ds):
    print(ds.attrs)
    ds = copy.deepcopy(ds)
    ds = ds.where(ds != ds.attrs['_FillValue'], 0)
    # rxr doesn't respect integer scaling when running selects, so we need to do it manually.
    # Might be nice to wrap this into our own rxr import
    ds = ds * ds.attrs['scale_factor'] + ds.attrs['add_offset']
    return ds