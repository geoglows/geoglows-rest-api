import os

from flask import jsonify
from main_controller import get_reach_from_latlon

# GLOBAL
PATH_TO_FORECASTS = '/mnt/output/forecasts'
PATH_TO_FORECAST_RECORDS = '/mnt/output/forecast-records'
PATH_TO_ERA_INTERIM = '/mnt/output/era-interim'
PATH_TO_ERA_5 = '/mnt/output/era-5'
M3_TO_FT3 = 35.3146667


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
        # if there is are dates in that folder
        if len(dates) != 0:
            # add it to the list of available data
            available_data[region] = dates
        else:
            available_data[region] = 'No Dates Discovered'

    return jsonify(available_data)


def get_region_handler():
    """
    Controller that returns available regions.
    """
    regions = os.listdir(PATH_TO_FORECASTS)

    if len(regions) > 0:
        return jsonify({"available_regions": regions})
    else:
        return jsonify({"message": "No regions found."}), 204


def get_reach_id_from_latlon_handler(request):
    """
    Controller that returns the reach_id nearest to valid lat/lon coordinates
    """
    lat = request.args.get('lat', '')
    lon = request.args.get('lon', '')
    return jsonify(get_reach_from_latlon(lat, lon))
