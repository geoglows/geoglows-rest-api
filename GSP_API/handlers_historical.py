import datetime
import glob
import json
import os

import hydrostats.data as hd
import xarray
from flask import jsonify, render_template, make_response
from functions import get_units_title, handle_parameters, find_historical_files

# GLOBAL
PATH_TO_FORECASTS = '/mnt/output/forecasts'
PATH_TO_FORECAST_RECORDS = '/mnt/output/forecast-records'
PATH_TO_ERA_INTERIM = '/mnt/output/era-interim'
PATH_TO_ERA_5 = '/mnt/output/era-5'
M3_TO_FT3 = 35.3146667


def historic_data_handler(request):
    """
    Controller for retrieving simulated historic data
    """
    # handle the parameters from the user
    try:
        reach_id, region, units, return_format = handle_parameters(request)
    except Exception as e:
        raise e
    forcing = request.args.get('forcing', 'era_5')
    historical_data_file, forcing_fullname = find_historical_files(region, forcing)

    # handle the units
    units_title, units_title_long = get_units_title(units)

    # collect the data in a dataframe
    qout_nc = xarray.open_dataset(historical_data_file)
    qout_data = qout_nc.sel(rivid=reach_id).Qout.to_dataframe()
    del qout_data['rivid'], qout_data['lon'], qout_data['lat']
    if units == 'english':
        qout_data['Qout'] *= M3_TO_FT3
    qout_data.rename(columns={'Qout': f'streamflow_{units_title}^3/s'}, inplace=True)
    qout_data.index = qout_data.index.strftime('%Y-%m-%dT%H:%M:%SZ')
    qout_data.index.name = 'datetime'

    # if csv, return the dataframe as csv
    if return_format == 'csv':
        response = make_response(qout_data.to_csv())
        response.headers['content-type'] = 'text/csv'
        response.headers['Content-Disposition'] = f'attachment; filename=historic_streamflow_{forcing}_{reach_id}.csv'
        return response

    # if you wanted json out, create and return json
    if return_format == "json":
        return {
            'region': region,
            'simulation_forcing': forcing,
            'forcing_fullname': forcing_fullname,
            'comid': reach_id,
            'gendate': datetime.datetime.utcnow().isoformat() + 'Z',
            'startdate': qout_data.index[0],
            'enddate': qout_data.index[-1],
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

    # todo waterml historic simulation
    # if return_format == "waterml":
    #     xml_response = make_response(render_template('historic_simulation.xml', **json_output))
    #     xml_response.headers.set('Content-Type', 'application/xml')
    #     return xml_response

    else:
        return jsonify({"error": "Invalid return_format."}), 422


def historic_averages_handler(request, average_type):
    """
    Controller for retrieving averages
    """
    # handle the parameters from the user
    try:
        reach_id, region, units, return_format = handle_parameters(request)
    except Exception as e:
        raise e
    forcing = request.args.get('forcing', 'era_5')
    historical_data_file, forcing_fullname = find_historical_files(region, forcing)

    # handle the units
    units_title, units_title_long = get_units_title(units)

    # collect the data in a dataframe
    qout_nc = xarray.open_dataset(historical_data_file)
    qout_data = qout_nc.sel(rivid=reach_id).Qout.to_dataframe()
    del qout_data['rivid'], qout_data['lon'], qout_data['lat']
    if units == 'english':
        qout_data['Qout'] *= M3_TO_FT3
    qout_data.rename(columns={'Qout': f'streamflow_{units_title}^3/s'}, inplace=True)

    if average_type == 'daily':
        qout_data = hd.daily_average(qout_data, rolling=True)
    else:
        qout_data = hd.monthly_average(qout_data)
    qout_data.index.name = 'datetime'

    if return_format == 'csv':
        response = make_response(qout_data.to_csv())
        response.headers['content-type'] = 'text/csv'
        response.headers['Content-Disposition'] = f'attachment; filename=seasonal_average_{forcing}_{reach_id}.csv'
        return response

    if return_format == "json":
        return jsonify({
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
        })

    # todo waterml historic averages
    # elif return_format == "waterml":
    #     xml_response = make_response(render_template('seasonal_averages.xml', **json_output))
    #     xml_response.headers.set('Content-Type', 'application/xml')
    #     return xml_response

    else:
        raise ValueError(f'Invalid return_format: {return_format}')


def return_periods_handler(request):
    """
    Controller for retrieving seasonal averages
    """
    # handle the parameters from the user
    try:
        reach_id, region, units, return_format = handle_parameters(request)
    except Exception as e:
        raise e
    forcing = request.args.get('forcing', 'era_5')

    if forcing == 'era_interim':
        forcing_fullname = 'ERA Interim'
        historical_data_file = glob.glob(os.path.join(PATH_TO_ERA_INTERIM, region, '*return_periods*.nc'))[0]
        startdate = '1980-01-01T00:00:00Z'
        enddate = '2014-12-31T00:00:00Z'
    elif forcing == 'era_5':
        forcing_fullname = 'ERA 5'
        historical_data_file = glob.glob(os.path.join(PATH_TO_ERA_5, region, '*return_periods*.nc'))[0]
        startdate = '1979-01-01T00:00:00Z'
        enddate = '2018-12-31T00:00:00Z'
    else:
        return {"error": "Invalid forcing specified, choose era_interim or era_5"}, 422

    # handle the units
    units_title, units_title_long = get_units_title(units)

    # collect the data in a dataframe
    qout_nc = xarray.open_dataset(historical_data_file)
    qout_data = qout_nc.to_dataframe()
    try:
        del qout_data['lon'], qout_data['lat']
    except Exception:
        pass
    qout_data = qout_data[qout_data.index == reach_id]
    if units == 'english':
        for column in qout_data:
            qout_data[column] *= M3_TO_FT3

    # if csv, return the dataframe as csv
    if return_format == 'csv':
        response = make_response(qout_data.to_csv())
        response.headers['content-type'] = 'text/csv'
        response.headers['Content-Disposition'] = f'attachment; filename=return_periods_{forcing}_{reach_id}.csv'
        return response

    # create a json of the data
    json_output = {
        'return_periods': json.loads(qout_data.to_json(orient='records'))[0],
        'region': region,
        'comid': reach_id,
        'simulation_forcing': forcing,
        'forcing_fullname': forcing_fullname,
        'gendate': datetime.datetime.utcnow().isoformat() + 'Z',
        'startdate': startdate,
        'enddate': enddate,
        'units': {
            'name': 'Streamflow',
            'short': f'{units_title}3/s',
            'long': f'Cubic {units_title_long} per Second'
        }
    }

    # if you wanted json out, return json
    if return_format == "json":
        return jsonify(json_output)

    # use the json to render a waterml document
    if return_format == "waterml":
        xml_response = make_response(render_template('return_periods.xml', **json_output))
        xml_response.headers.set('Content-Type', 'application/xml')
        return xml_response

    else:
        return jsonify({"error": "Invalid return_format."}), 422
