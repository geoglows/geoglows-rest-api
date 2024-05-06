import geoglows
from flask import jsonify

__all__ = [
    'get_river_id',
]


def get_river_id(lat: float, lon: float):
    """
    Finds the river ID nearest to a given lat/lon
    Uses the ModelMasterTable to find the locations
    """
    if not lat or not lon:
        raise ValueError('Provide both lat and lon arguments')
    river_id = geoglows.streams.latlon_to_river(lat=float(lat), lon=float(lon))
    return jsonify(dict(river_id=int(river_id)))
