import os

from app.v1.deprecation_utilities import add_deprecation_warning_json
from flask import jsonify

from .constants import PATH_TO_FORECASTS
from .v1_functions import latlon_to_reach

__all__ = ['get_available_data_handler', 'get_region_handler', 'get_reach_id_from_latlon_handler']


def get_available_data_handler():
    available_data = {}

    # get a list of the available regions
    regions = os.listdir(PATH_TO_FORECASTS)
    if len(regions) == 0:
        return jsonify({'error': 'no regions were found'})
    available_data['Total_Regions'] = len(regions)

    # for each region
    for region in regions:
        region_path = os.path.join(PATH_TO_FORECASTS, region)
        # get a list of the data in its folder
        dates = [d for d in os.listdir(region_path) if d.split('.')[0].isdigit()]
        # if there are dates in that folder
        if len(dates) != 0:
            # add it to the list of available data
            available_data[region] = dates
        else:
            available_data[region] = 'No Dates Discovered'

    return jsonify(add_deprecation_warning_json(available_data))


def get_region_handler():
    """
    Controller that returns available regions.
    """
    regions = os.listdir(PATH_TO_FORECASTS)
    
    if len(regions) > 0:
        response_data = add_deprecation_warning_json({"available_regions": regions})
        return jsonify(response_data)
    else:
        response_data = add_deprecation_warning_json({"message": "No regions found."})
        return jsonify(response_data), 204


def get_reach_id_from_latlon_handler(request):
    """
    Controller that returns the reach_id nearest to valid lat/lon coordinates
    """
    if request.args.get('lat', '') == '' or request.args.get('lon', '') == '':
        raise ValueError('Specify both a latitude (lat) and a longitude (lon)')
    lat = request.args.get('lat', '')
    lon = request.args.get('lon', '')
    reach_id, region, dist_error = latlon_to_reach(lat, lon)
    response_data = add_deprecation_warning_json(dict(reach_id=reach_id, region=region, dist_error=dist_error))
    return jsonify(response_data)
