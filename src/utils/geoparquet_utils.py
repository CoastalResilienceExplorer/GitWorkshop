import geopandas as gpd
import pandas as pd
# import s2sphere
import uuid
import gc

COUNTRY_PARTITION_FILE="gs://geopmaker-output-dev/vectors/World_Countries_Generalized.parquet"


def is_polygon(gdf):
    polygon_bools = gdf.geom_type.apply(lambda s: "polygon" in s.lower()).unique()
    return len(polygon_bools) == 1 and polygon_bools[0]


def partition_gdf(
    gdf,
    partition_cols=[],
    partition_by_s2=True,
    partition_by_country=True,
):
    cols = partition_cols
    if "ISO3" in gdf.columns:
        partitioned_gdf = gdf.rename(columns={"ISO3": "ISO"})
        cols += ["ISO"]
    elif partition_by_country:
        print('partition by country')
        partitions = gpd.read_parquet(COUNTRY_PARTITION_FILE)
        print('partitions read')
        partitions = partitions[["geometry", "ISO"]]
        xmin, ymin, xmax, ymax = bounds = gdf.total_bounds
        print(bounds)
        partitions = partitions.cx[xmin: ymin, xmax: ymax]
        partitioned_gdf = gpd.sjoin(gdf, partitions, how="left")
        print('joined')
        print(partitioned_gdf.shape)
        # Swap ISO2 for ISO3, since that is generally more common
        iso_mappings = pd.read_csv("countries-codes.csv")
        partitioned_gdf = (
            pd.merge(
                partitioned_gdf, iso_mappings, left_on="ISO", right_on="ISO2 CODE"
            )
            .drop(columns=["ISO"])
            .rename(columns={"ISO3 CODE": "ISO"})
        )
        cols += ["ISO"]
    else:
        partitioned_gdf = gdf.copy()

    def get_bounds_by_geom(geom):
        bounds = geom.bounds
        p1 = s2sphere.LatLng.from_degrees(max(bounds[1], -90), max(bounds[0], -180))
        p2 = s2sphere.LatLng.from_degrees(min(bounds[3], 90), min(bounds[2], 180))
        if not p1.is_valid():
            print("p1")
            print(p1)
        if not p2.is_valid():
            print("p2")
            print(p2)
        cell_ids = [
            str(i.id())
            for i in r.get_covering(s2sphere.LatLngRect.from_point_pair(p1, p2))
        ]
        return cell_ids

    if partition_by_s2:
        r = s2sphere.RegionCoverer()
        r.min_level = 5
        r.max_level = 7
        print('started s2 partition')
        partitioned_gdf["s2"] = partitioned_gdf.geometry.apply(
            lambda g: get_bounds_by_geom(g)
        )
        print('finished s2 partition')
        partitioned_gdf = partitioned_gdf.explode("s2")
        cols += ["s2"]

    return partitioned_gdf, cols


def write_partitioned_gdf(gdf, output, cols=[]):
    tmp_id = str(uuid.uuid1())
    tmp_parquet = f"/tmp/{tmp_id}.parquet"
    gdf.to_parquet(tmp_parquet)
    print('tmp created')
    del gdf
    gc.collect()
    to_write = pd.read_parquet(tmp_parquet)
    print(to_write)
    print(to_write.columns)
    to_write.to_parquet(
        output, partition_cols=cols, max_partitions=1_000_000
    )