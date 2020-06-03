import datetime

import xarray
from flask import jsonify, render_template, make_response
from functions import get_units_title, handle_parameters, find_historical_files

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
    # handle the parameters from the user
    reach_id, region, units, return_format = handle_parameters(request)
    forcing = request.args.get('forcing', 'era_5')
    historical_data_file, forcing_fullname = find_historical_files(region, forcing)

    # handle the units
    units_title, units_title_long = get_units_title(units)

    # collect the data in a dataframe
    qout_nc = xarray.open_dataset(historical_data_file)
    qout_data = qout_nc.sel(rivid=reach_id).Qout.to_dataframe()
    del qout_data['rivid'], qout_data['lon'], qout_data['lat']
    qout_data.index = qout_data.index.strftime('%Y-%m-%dT%H:%M:%SZ')
    qout_data.index.name = 'datetime'
    if units == 'english':
        qout_data['Qout'] *= M3_TO_FT3
    qout_data.rename(columns={'Qout': f'streamflow_{units_title}^3/s'}, inplace=True)

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

