import requests
import json
import logging

logging.basicConfig()
logging.root.setLevel(logging.INFO)

def overpass_api_request(query):
    overpass_url = "http://overpass-api.de/api/interpreter"
    response = requests.post(overpass_url, data=query)
    print(response.content)
    return response.json()

def get_ways(bbox, way_type):
    overpass_query = f"""
        [out:json];
        (
          way["{way_type}"]({bbox});
        );
        out body geom;
        >;
        out skel qt;
    """
    return overpass_api_request(overpass_query)

def is_closed_way(way):
    # Check if the first and last nodes are the same
    return way["nodes"][0] == way["nodes"][-1]

def geom_to_coords(geom):
    return [geom['lon'], geom['lat']]

def main(left, bottom, top, right, way_type):
    # Define the bounding box (min_longitude, min_latitude, max_longitude, max_latitude)
    bounding_box = f"{bottom},{left},{top},{right}"

    # Get buildings and roads within the bounding box
    result = get_ways(bounding_box, way_type)

    # Create a GeoJSON FeatureCollection
    features = []

    for element in result["elements"]:
        geom = element.get("geometry", None)
        if geom:
            geom = [[c['lon'], c['lat']] for c in geom]
        else:
            continue

        if element["type"] == "way":
            # Check if the way is closed (building/polygon) or open (highway/line)
            is_polygon = is_closed_way(element)
            geometry_type = "Polygon" if is_polygon else "LineString"
        else:
            # Nodes are treated as points
            geometry_type = "Point"

        feature = {
            "type": "Feature",
            "geometry": {
                "type": geometry_type,
                "coordinates": [geom] if geometry_type == "Polygon" else geom
            },
            "properties": {
                "id": element["id"],
                "type": element["type"],
                **element.get("tags", dict())
            }
        }
        features.append(feature)

    return {
        "type": "FeatureCollection",
        "features": features
    }

if __name__ == "__main__":
    main()
