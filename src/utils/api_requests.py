from functools import wraps
import logging
from flask import request
import flask
import geopandas as gpd
import uuid, os
import xarray as xr
import numpy as np
from utils.gcs import upload_blob
from utils.dataset import compressRaster, maskEdge
from utils.geoparquet_utils import write_partitioned_gdf

logging.basicConfig()
logging.root.setLevel(logging.INFO)

TMP_FOLDER='/tmp'
GCS_BASE_RASTER=os.environ['OUTPUT_BUCKET_RASTER']
GCS_BASE_VECTOR=os.environ['OUTPUT_BUCKET_VECTOR']
# MNT_BASE=os.environ['MNT_BASE']

def data_to_parameters_factory(app):
    def data_to_parameters(func):
        with app.test_request_context():
            @wraps(func)
            def wrapper(*args, **kwargs):
                """A wrapper function"""
                # Extend some capabilities of func
                data = request.get_json()
                to_return = func(**data)
                return to_return
            return wrapper
    return data_to_parameters


def response_to_gpkg_factory(app):
    def response_to_gpkg(func):
        with app.test_request_context():
            @wraps(func)
            def wrapper(*args, **kwargs):
                """A wrapper function"""
                xid = str(uuid.uuid1())
                # Extend some capabilities of func
                gdf_to_return = func(*args, **kwargs)
                assert isinstance(gdf_to_return, gpd.GeoDataFrame)
                fname=f'{xid}.gpkg'
                gdf_to_return.to_file(os.path.join(TMP_FOLDER, fname))
                return flask.send_from_directory(TMP_FOLDER, fname)

            return wrapper
    return response_to_gpkg


def response_to_tiff_factory(app):
    def response_to_tiff(func):
        with app.test_request_context():
            @wraps(func)
            def wrapper(*args, **kwargs):
                id = str(uuid.uuid4())
                # Extend some capabilities of func
                xr_to_return = func(*args, **kwargs)
                assert isinstance(xr_to_return, xr.DataArray) | isinstance(xr_to_return, xr.Dataset)
                fname = f"{id}.tiff"
                tmp_rast_compressed = os.path.join(TMP_FOLDER, fname)
                compressRaster(xr_to_return, tmp_rast_compressed)
                data = request.form
                if "output_to_gcs" in data.keys():
                    upload_blob(GCS_BASE_RASTER, tmp_rast_compressed, request.form['output_to_gcs'])
                return flask.send_from_directory(TMP_FOLDER, fname)
        return wrapper
    return response_to_tiff


def response_to_tiff(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        """A wrapper function"""
        id = str(uuid.uuid4())
        # Extend some capabilities of func
        xr_to_return = func(*args, **kwargs)
        assert isinstance(xr_to_return, xr.DataArray) | isinstance(xr_to_return, xr.Dataset)
        fname = f"{id}.tiff"
        tmp_rast_compressed = os.path.join(TMP_FOLDER, fname)
        compressRaster(xr_to_return, tmp_rast_compressed)
        return flask.send_from_directory(TMP_FOLDER, fname)
    
    return wrapper


def process_reprojection_edge(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        """A wrapper function"""
        xr_to_return = func(*args, **kwargs)
        return maskEdge(xr_to_return)
    
    return wrapper

def nodata_to_zero(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        """A wrapper function"""
        xr_to_return = func(*args, **kwargs)
        x = xr.where(xr_to_return == xr_to_return.rio.nodata, 0, xr_to_return)
        x = x.rio.write_crs(xr_to_return.rio.crs)
        x = x.rio.write_nodata(0)
        return x
    return wrapper