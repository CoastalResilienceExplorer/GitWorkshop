from utils.geo import transform_point, geojson_to_geodataframe, extract_z_values
from utils.cache import memoize_with_persistence
from utils.osm import main as _get_osm

import geopandas as gpd
import pandas as pd
import s2sphere
import os
import copy
import logging

logging.basicConfig()
logging.root.setLevel(logging.INFO)


def gpd_read_parquet(path):
    data = pd.read_parquet(path)
    gdf = gpd.GeoDataFrame(data, geometry=gpd.GeoSeries.from_wkb(data["geometry"]))
    return gdf


gpd.read_parquet = gpd_read_parquet


def get_covering(lower_left, upper_right):
    r = s2sphere.RegionCoverer()
    p1 = s2sphere.LatLng.from_degrees(lower_left[1], lower_left[0])
    p2 = s2sphere.LatLng.from_degrees(upper_right[1], upper_right[0])
    covering = r.get_covering(s2sphere.LatLngRect.from_point_pair(p1, p2))
    return covering


def get_relevant_partitions(covering, features):
    partition_ids = [int(i.split(".")[0]) for i in os.listdir(features)]
    buff = []
    for p in partition_ids:
        p = s2sphere.CellId(p)
        for c in covering:
            if p.contains(c) or c.contains(p) or p == c:
                buff.append(p)
    return list(set(buff))



@memoize_with_persistence("/tmp/cache.pkl")
def get_bbox_filtered_gdf(features, lower_left, upper_right) -> gpd.GeoDataFrame:
    logging.info(lower_left)
    logging.info(upper_right)
    covering = get_covering(
        [lower_left.x, lower_left.y], [upper_right.x, upper_right.y]
    )

    relevant_partitions = get_relevant_partitions(covering, features)
    logging.info(relevant_partitions)
    buff = []
    for p in relevant_partitions:
        buff.append(gpd.read_parquet(os.path.join(features, f"{p.id()}.parquet")))
    gdf = pd.concat(buff)
    print(gdf.shape)
    gdf_filtered = gdf.cx[lower_left.x : upper_right.x, lower_left.y : upper_right.y]
    return gdf_filtered


# @memoize_with_persistence('/tmp/cache')
def get_features_unpartitioned(features_file, left, bottom, right, top, CRS):
    lower_left = transform_point(left, bottom, CRS)
    upper_right = transform_point(right, top, CRS)
    features = gpd.read_parquet(
        os.path.join(os.environ["MNT_BASE"], features_file)
    )
    print(features)
    print(lower_left, upper_right)
    features = features.cx[lower_left.x:upper_right.x, lower_left.y:upper_right.y]
    return features


def get_open_buildings(left, bottom, right, top, ISO3, crs):
    features_file=f"supporting-data2/google-microsoft-open-buildings.parquet/country_iso={ISO3}/"
    lower_left = transform_point(left, bottom, crs)
    upper_right = transform_point(right, top, crs)
    buildings = get_bbox_filtered_gdf(
        os.path.join(os.environ["GCS_MNT_BASE"], features_file),
        lower_left,
        upper_right,
    )
    return buildings


@memoize_with_persistence("/tmp/cache.pkl")
def get_osm(left, bottom, top, right, way_type):
    data = _get_osm(left, bottom, top, right, way_type)
    gdf = geojson_to_geodataframe(data)
    return gdf


def get_features_with_z_values(ds, id="flooding", features_from="OPEN_BUILDINGS", way_type="building", ISO3="USA"):
    assert features_from in ('OSM, OPEN_BUILDINGS')
    if features_from == "OSM":
        b = ds.rio.bounds()
        left, bottom, right, top = b
        gdf = get_osm(left=left, bottom=bottom, right=right, top=top, way_type=way_type)
        gdf = gdf[['id', 'type', 'geometry']]
        gdf_points = copy.deepcopy(gdf)
        gdf_points['geometry'] = gdf_points['geometry'].centroid
        gdf_points = extract_z_values(ds=ds, gdf=gdf_points, column_name=id)
        gdf[id] = gdf_points[id]
        # gdf[id][gdf[id] == z.rio.nodata] = np.nan
        return gdf
    
    if features_from == "OPEN_BUILDINGS":
        b = ds.rio.bounds()
        left, bottom, right, top = b
        logging.info(left)
        logging.info(bottom)
        logging.info(right)
        logging.info(top)
        logging.info(ds.rio.crs)
        gdf = get_open_buildings(left=left, bottom=bottom, right=right, top=top, ISO3=ISO3, crs=ds.rio.crs)
        gdf_points = copy.deepcopy(gdf)
        logging.info(gdf_points)
        gdf_points['geometry'] = gdf_points['geometry'].centroid
        gdf_points = extract_z_values(ds=ds, gdf=gdf_points, column_name=id)
        gdf[id] = gdf_points[id]
        # gdf[id][gdf[id] == ds.rio.nodata] = np.nan
        return gdf
    
    else:
        return ("Only OSM currently supported", 404)


# @memoize_with_persistence("/tmp/cache.pkl")
# def get_osm(left, bottom, top, right, way_type):
#     data = _get_osm(left, bottom, top, right, way_type)
#     gdf = geojson_to_geodataframe(data)
#     return gdf