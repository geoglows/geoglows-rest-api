import geoglows
from flask import jsonify

from .constants import PACKAGE_METADATA_TABLE_PATH

__all__ = [
    'get_reach_id',
]

geoglows.METADATA_TABLE_PATH = PACKAGE_METADATA_TABLE_PATH


def get_reach_id(lat: float, lon: float):
    """
    Finds the reach ID nearest to a given lat/lon
    Uses the ModelMasterTable to find the locations
    """
    if not lat or not lon:
        raise ValueError('Provide both lat and lon arguments')
    lat = float(lat)
    lon = float(lon)
    reach_id = geoglows.streams.latlon_to_reach(lat, lon)
    return jsonify(dict(reach_id=reach_id))
