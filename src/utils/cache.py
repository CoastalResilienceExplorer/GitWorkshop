import pickle
import json
from functools import wraps
import geopandas as gpd
import xarray as xr

def memoize_with_persistence(filename):
    try:
        with open(filename, 'rb') as file:
            cache = pickle.load(file)
    except (FileNotFoundError, pickle.UnpicklingError):
        cache = {}

    def memoize(func):

        def wrapper(*args, **kwargs):
            # Use a tuple of args and frozenset of kwargs as the cache key
            key = (args, frozenset(kwargs.items()))

            if key in cache:
                # If the result is already in the cache, return it
                print(f"Cache hit for {func.__name__} with key {key}")
                return cache[key]
            else:
                # Otherwise, compute the result and store it in the cache
                result = func(*args, **kwargs)
                cache[key] = result
                print(f"Cache miss for {func.__name__} with key {key}")

                # Save the updated cache to the pickled file
                with open(filename, 'wb') as file:
                    pickle.dump(cache, file)

                return result

        return wrapper
    
    return memoize


def memoize(func):
    cache = {}

    def wrapper(*args, **kwargs):
        # Use a tuple of args and frozenset of kwargs as the cache key
        key = (args, frozenset(kwargs.items()))

        if key in cache:
            # If the result is already in the cache, return it
            print(f"Cache hit for {func.__name__} with key {key}")
            return cache[key]
        else:
            # Otherwise, compute the result and store it in the cache
            result = func(*args, **kwargs)
            cache[key] = result
            print(f"Cache miss for {func.__name__} with key {key}")
            return result

    return wrapper


def memoize_geospatial_with_persistence(filename):
    try:
        with open(filename, 'rb') as file:
            cache = pickle.load(file)
    except (FileNotFoundError, pickle.UnpicklingError):
        cache = {}

    def wrapper(func):
        @wraps(func)
        def inner(*args, **kwargs):
            # Serialize the arguments and use them as the cache key
            key_args = tuple(json.dumps(arg, default=lambda o: o.to_dict() if hasattr(o, 'to_dict') else o) for arg in args)
            key = (key_args, frozenset(kwargs.items()))

            if key in cache:
                # If the key is already in the cache, return the result
                cached_result = cache[key]
                obj_type, cached_value = cached_result['type'], cached_result['value']

                return_type = func.__annotations__.get('return', None)
                if obj_type == 'geopandas' and return_type == gpd.GeoDataFrame:
                    # If the object was a GeoDataFrame, reconstruct it
                    result = gpd.GeoDataFrame.from_features(cached_value)
                elif obj_type == 'xarray' and return_type == xr.DataArray:
                    # If the object was a DataArray, reconstruct it
                    result = xr.DataArray.from_dict(cached_value)
                else:
                    # If the object was not a recognized type, use it directly
                    result = cached_value

                print(f"Cache hit for {func.__name__}")
                return result
            else:
                # Otherwise, compute the result and store it in the cache
                result = func(*args, **kwargs)

                # Determine the type of the result and store accordingly
                return_type = func.__annotations__.get('return', None)
                if isinstance(result, gpd.GeoDataFrame) and return_type == gpd.GeoDataFrame:
                    cache[key] = {'type': 'geopandas', 'value': result.__geo_interface__}
                elif isinstance(result, xr.DataArray) and return_type == xr.DataArray:
                    cache[key] = {'type': 'xarray', 'value': result.to_dict()}
                else:
                    cache[key] = {'type': 'other', 'value': result}

                print(f"Cache miss for {func.__name__}")

                # Save the updated cache to the pickled file
                with open(filename, 'wb') as file:
                    pickle.dump(cache, file)

                return result

        return inner

    return wrapper