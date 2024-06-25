from pystac_client import Client
from datetime import datetime
import requests
import rioxarray as rxr
import xarray as xr

# Function to search for Sentinel-2 assets given a specific location and time range
def get_landuse(location, year):
    catalog = Client.open('https://planetarycomputer.microsoft.com/api/stac/v1')
    results = catalog.search(
        max_items=5,
        bbox=location,
        datetime=[f'{year}-01-01T00:00:00Z', f'{year+1}-01-02T00:00:00Z'],
        collections=['io-lulc-9-class']
    )
    buff = []
    for i in results.items():
        print(i)
        buff.append(i.assets['data'].href)
    return buff

# Function to download Sentinel-2 asset given the item
def download_and_compile_items(items):
    buff = []
    for item in items:
        x = rxr.open_rasterio(item)
        x.name=item
        buff.append(x)
    return xr.merge(buff)

# # Example usage
# # location = [-180, -90, 180, 90]  # Coordinates defining the entire globe
# location = [-91.574, 15.089, -84.395, 19.744]
# start_date = datetime(2023, 1, 1)
# end_date = datetime(2023, 12, 31)

# # Search for Sentinel-2 assets
# items = search_sentinel2_assets(location, start_date, end_date)

# # Iterate through the found items
# for item in items:
#     # Download each asset
#     asset_url = download_sentinel2_asset(item)
#     print(f"Downloaded asset: {asset_url}")