import geoglows
from flask import jsonify

__all__ = [
    'get_reach_id',
]


def get_reach_id(lat: float, lon: float):
    """
    Finds the reach ID nearest to a given lat/lon
    Uses the ModelMasterTable to find the locations
    """
    if not lat or not lon:
        raise ValueError('Provide both lat and lon arguments')
    reach_id = geoglows.streams.latlon_to_reach(lat=float(lat), lon=float(lon))
    return jsonify(dict(reach_id=int(reach_id)))
