import datetime
import glob
import os

import xarray
from flask import jsonify, render_template, make_response
from functions import get_units_title, reach_to_region, latlon_to_reach

# GLOBAL
PATH_TO_FORECASTS = '/mnt/output/forecasts'
PATH_TO_FORECAST_RECORDS = '/mnt/output/forecast-records'
PATH_TO_ERA_INTERIM = '/mnt/output/era-interim'
PATH_TO_ERA_5 = '/mnt/output/era-5'
M3_TO_FT3 = 35.3146667


def seasonal_average_handler(request):
    """
    Controller for retrieving seasonal averages
    """
    reach_id = int(request.args.get('reach_id', False))
    lat = request.args.get('lat', False)
    lon = request.args.get('lon', False)
    units = request.args.get('units', 'metric')
    forcing = request.args.get('forcing', 'era_5')
    return_format = request.args.get('return_format', 'csv')

    if reach_id:
        region = reach_to_region(reach_id)
    elif lat and lon:
        reach_id, region, dist_error = latlon_to_reach(lat, lon)
    else:
        return {"error": "Invalid reach_id or lat/lon/region combination"}, 422

    if forcing == 'era_interim':
        forcing_fullname = 'ERA Interim'
        seasonal_data_file = glob.glob(os.path.join(PATH_TO_ERA_INTERIM, region, 'seasonal_average*.nc'))[0]
    elif forcing == 'era_5':
        forcing_fullname = 'ERA 5'
        seasonal_data_file = glob.glob(os.path.join(PATH_TO_ERA_5, region, 'seasonal_average*.nc'))[0]
    else:
        return {"error": "Invalid forcing specified, choose era_interim or era_5"}, 422

    # handle the units
    units_title, units_title_long = get_units_title(units)

    # collect the data in a dataframe
    qout_nc = xarray.open_dataset(seasonal_data_file)
    qout_data = qout_nc.sel(rivid=reach_id).to_dataframe()
    del qout_data['rivid'], qout_data['lon'], qout_data['lat']
    qout_data.index.rename('day_of_year', inplace=True)
    qout_data.rename(columns={'average_flow': f'streamflow_{units_title}^3/s'}, inplace=True)
    if units == 'english':
        for column in qout_data:
            qout_data[column] *= M3_TO_FT3

    if return_format == 'csv':
        response = make_response(qout_data.to_csv())
        response.headers['content-type'] = 'text/csv'
        response.headers['Content-Disposition'] = f'attachment; filename=seasonal_average_{forcing}_{reach_id}.csv'
        return response

    json_output = {
        'region': region,
        'simulation_forcing': forcing,
        'forcing_fullname': forcing_fullname,
        'comid': reach_id,
        'gendate': datetime.datetime.utcnow().isoformat() + 'Z',
        'time_series': {
            'datetime': qout_data.index.tolist(),
            'flow': qout_data[f'streamflow_{units_title}^3/s'].tolist(),
        },
        'units': {
            'name': 'Streamflow',
            'short': f'{units_title}3/s',
            'long': f'Cubic {units_title_long} per Second'
        }
    }

    if return_format == "json":
        return jsonify(json_output)

    elif return_format == "waterml":
        xml_response = make_response(render_template('seasonal_averages.xml', **json_output))
        xml_response.headers.set('Content-Type', 'application/xml')
        return xml_response

    else:
        return jsonify({"error": "Invalid return_format."}), 422
