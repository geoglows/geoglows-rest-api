import datetime
import glob
import logging
import os
from csv import writer as csv_writer
from io import StringIO

import pandas as pd
import xarray
from flask import jsonify, render_template, make_response
from functions import get_units_title, reach_to_region, latlon_to_reach, ecmwf_find_most_current_files

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


def get_ecmwf_forecast_statistics(request):
    """
    Returns the statistics for the 52 member forecast
    """
    reach_id = int(request.args.get('reach_id', False))
    stat_type = request.args.get('stat', '')
    lat = request.args.get('lat', '')
    lon = request.args.get('lon', '')
    forecast_folder = request.args.get('date', 'most_recent')
    units = request.args.get('units', 'metric')

    if reach_id:
        region = reach_to_region(reach_id)
        if not region:
            return {"error": "Unable to determine a region paired with this reach_id"}
    elif lat != '' and lon != '':
        reach_id, dist_error = latlon_to_reach(lat, lon)
        if dist_error:
            return dist_error
    else:
        return {"error": "Invalid reach_id or lat/lon/region combination"}, 422

    # find/check current output datasets
    path_to_output_files = os.path.join(PATH_TO_FORECASTS, region)
    forecast_nc_list, start_date = ecmwf_find_most_current_files(path_to_output_files, forecast_folder)
    if not forecast_nc_list or not start_date:
        return {"error": 'ECMWF forecast for ' + region}, 422
    print('1')

    # combine 52 ensembles
    qout_datasets = []
    ensemble_index_list = []
    for forecast_nc in forecast_nc_list:
        ensemble_index_list.append(int(os.path.basename(forecast_nc)[:-3].split("_")[-1]))
        qout_datasets.append(xarray.open_dataset(forecast_nc).sel(rivid=reach_id).Qout)

    merged_ds = xarray.concat(qout_datasets, pd.Index(ensemble_index_list, name='ensemble'))
    print('2')

    return_dict = {}
    if stat_type in ('high_res', 'all') or not stat_type:
        # extract the high res ensemble & time
        try:
            return_dict['high_res'] = merged_ds.sel(ensemble=52).dropna('time')
        except:
            pass

    if stat_type != 'high_res' or not stat_type:
        # analyze data to get statistic bands
        merged_ds = merged_ds.dropna('time')

        if stat_type == 'mean' or 'std' in stat_type or not stat_type:
            return_dict['mean'] = merged_ds.mean(dim='ensemble')
            std_ar = merged_ds.std(dim='ensemble')
            if stat_type == 'std_dev_range_upper' or not stat_type:
                return_dict['std_dev_range_upper'] = return_dict['mean'] + std_ar
            if stat_type == 'std_dev_range_lower' or not stat_type:
                return_dict['std_dev_range_lower'] = return_dict['mean'] - std_ar
        if stat_type == "min" or not stat_type:
            return_dict['min'] = merged_ds.min(dim='ensemble')
        if stat_type == "max" or not stat_type:
            return_dict['max'] = merged_ds.max(dim='ensemble')

    for key in list(return_dict):
        if units == 'english':
            # convert m3/s to ft3/s
            return_dict[key] *= M3_TO_FT3
        # convert to pandas series
        return_dict[key] = return_dict[key].to_dataframe().Qout

    return return_dict, region, reach_id, units


def get_forecast_streamflow_csv(request):
    """
    Retrieve the forecasted streamflow as CSV
    """

    try:
        # retrieve statistics
        forecast_statistics, region, reach_id, units = get_ecmwf_forecast_statistics(request)

        forecast_df = pd.DataFrame(forecast_statistics)

        response = make_response(forecast_df.to_csv())
        response.headers['content-type'] = 'text/csv'
        response.headers['Content-Disposition'] = \
            'attachment; filename=forecasted_streamflow_{0}_{1}.csv'.format(region, reach_id)

        return response

    except:
        return {"error": "An unexpected error occurred with the CSV response."}, 422


def deprecated_forecast_stats_handler(request):
    """
    Controller that will retrieve forecast statistic data
    in different formats
    """

    return_format = request.args.get('return_format', 'csv')

    if return_format in ('csv', ''):
        csv_response = get_forecast_streamflow_csv(request)
        if isinstance(csv_response, dict) and "error" in csv_response.keys():
            return jsonify(csv_response)
        else:
            return csv_response

    elif return_format in ('waterml', 'json'):

        formatted_stat = {
            'high_res': 'High Resolution',
            'mean': 'Mean',
            'min': 'Min',
            'max': 'Max',
            'std_dev_range_lower': 'Standard Deviation Lower Range',
            'std_dev_range_upper': 'Standard Deviation Upper Range',
        }

        # retrieve statistics
        forecast_statistics, region, river_id, units = get_ecmwf_forecast_statistics(request)

        units_title = get_units_title(units)
        units_title_long = 'Meters'
        if units_title == 'ft':
            units_title_long = 'Feet'

        stat = request.args.get('stat', '')

        context = {
            'region': region,
            'comid': river_id,
            'gendate': datetime.datetime.utcnow().isoformat() + 'Z',
            'units': {
                'name': 'Streamflow',
                'short': '{}3/s'.format(units_title),
                'long': 'Cubic {} per Second'.format(units_title_long)
            }
        }

        stat_ts_dict = {}
        if stat != '' and stat != 'all':

            if stat not in formatted_stat.keys():
                logging.error('Invalid value for stat ...')
                return jsonify({"error": "Invalid value for stat parameter."}), 422

            startdate = forecast_statistics[stat].index[0] \
                .strftime('%Y-%m-%dT%H:%M:%SZ')
            enddate = forecast_statistics[stat].index[-1] \
                .strftime('%Y-%m-%dT%H:%M:%SZ')

            time_series = []
            for date, value in forecast_statistics[stat].items():
                time_series.append({
                    'date': date.strftime('%Y-%m-%dT%H:%M:%SZ'),
                    'val': value
                })

            context['stats'] = {stat: formatted_stat[stat]}
            context['startdate'] = startdate
            context['enddate'] = enddate
            stat_ts_dict[stat] = time_series

        else:
            startdate = forecast_statistics['mean'].index[0] \
                .strftime('%Y-%m-%dT%H:%M:%SZ')
            enddate = forecast_statistics['mean'].index[-1] \
                .strftime('%Y-%m-%dT%H:%M:%SZ')

            high_r_time_series = []
            for date, value in forecast_statistics['high_res'].items():
                high_r_time_series.append({
                    'date': date.strftime('%Y-%m-%dT%H:%M:%SZ'),
                    'val': value
                })

            mean_time_series = []
            for date, value in forecast_statistics['mean'].items():
                mean_time_series.append({
                    'date': date.strftime('%Y-%m-%dT%H:%M:%SZ'),
                    'val': value
                })

            max_time_series = []
            for date, value in forecast_statistics['max'].items():
                max_time_series.append({
                    'date': date.strftime('%Y-%m-%dT%H:%M:%SZ'),
                    'val': value
                })

            min_time_series = []
            for date, value in forecast_statistics['min'].items():
                min_time_series.append({
                    'date': date.strftime('%Y-%m-%dT%H:%M:%SZ'),
                    'val': value
                })

            std_d_lower_time_series = []
            for date, value in forecast_statistics['std_dev_range_lower'].items():
                std_d_lower_time_series.append({
                    'date': date.strftime('%Y-%m-%dT%H:%M:%SZ'),
                    'val': value
                })

            std_d_upper_time_series = []
            for date, value in forecast_statistics['std_dev_range_upper'].items():
                std_d_upper_time_series.append({
                    'date': date.strftime('%Y-%m-%dT%H:%M:%SZ'),
                    'val': value
                })

            context['stats'] = formatted_stat

            context['startdate'] = startdate
            context['enddate'] = enddate
            stat_ts_dict['high_res'] = high_r_time_series
            stat_ts_dict['mean'] = mean_time_series
            stat_ts_dict['max'] = max_time_series
            stat_ts_dict['min'] = min_time_series
            stat_ts_dict['std_dev_range_lower'] = std_d_lower_time_series
            stat_ts_dict['std_dev_range_upper'] = std_d_upper_time_series

        context['time_series'] = stat_ts_dict

        if return_format == "waterml":
            xml_response = \
                make_response(render_template('forecast_stats.xml', **context))
            xml_response.headers.set('Content-Type', 'application/xml')

            return xml_response

        if return_format == "json":
            return jsonify(context)

    else:
        return jsonify({"error": "Invalid return_format."}), 422


def get_historic_streamflow_series(request):
    """
    Retrieve Pandas series object based on request for ERA Interim data
    """
    reach_id = int(request.args.get('reach_id', False))
    lat = request.args.get('lat', '')
    lon = request.args.get('lon', '')
    daily = request.args.get('daily', '')
    units = request.args.get('units', 'metric')

    if reach_id:
        region = reach_to_region(reach_id)
        if not region:
            return {"error": "Unable to determine a region paired with this reach_id"}
    elif lat != '' and lon != '':
        reach_id, dist_error = latlon_to_reach(lat, lon)
        if dist_error:
            return dist_error
    else:
        return {"error": "Invalid reach_id or lat/lon/region combination"}, 422

    historical_data_file = glob.glob(os.path.join(PATH_TO_ERA_INTERIM, region, 'Qout*.nc'))[0]

    # write data to csv stream
    with xarray.open_dataset(historical_data_file) as qout_nc:
        qout_data = qout_nc.sel(rivid=reach_id).Qout.to_dataframe().Qout
        if daily.lower() == 'true':
            # calculate daily values
            qout_data = qout_data.resample('D').mean()

        if units == 'english':
            # convert from m3/s to ft3/s
            qout_data *= M3_TO_FT3
    return qout_data, region, reach_id, units


def get_historic_data_csv(request):
    """""
    Returns ERA Interim data as csv
    """""

    try:
        qout_data, region, reach_id, units = get_historic_streamflow_series(request)

        si = StringIO()
        writer = csv_writer(si)

        writer.writerow(['datetime', 'streamflow ({}3/s)'.format(get_units_title(units)[0])])

        for row_data in qout_data.items():
            writer.writerow(row_data)

        # prepare to write response
        response = make_response(si.getvalue())
        response.headers['content-type'] = 'text/csv'
        response.headers['Content-Disposition'] = \
            'attachment; filename=historic_streamflow_{0}_{1}.csv'.format(region, reach_id)

        return response
    except:
        return {"error": "An unexpected error occurred with the CSV response."}, 422


def deprecated_historic_data_handler(request):
    """
    Controller for retrieving simulated historic data
    """
    return_format = request.args.get('return_format', 'csv')

    if return_format in ('csv', ''):
        csv_response = get_historic_data_csv(request)
        if isinstance(csv_response, dict) and "error" in csv_response.keys():
            return jsonify(csv_response)
        else:
            return csv_response

    elif return_format in ('waterml', 'json'):

        qout_data, region, reach_id, units = get_historic_streamflow_series(request)

        units_title = get_units_title(units)
        units_title_long = 'meters'
        if units_title == 'ft':
            units_title_long = 'feet'

        context = {
            'region': region,
            'comid': reach_id,
            'gendate': datetime.datetime.utcnow().isoformat() + 'Z',
            'units': {
                'name': 'Streamflow',
                'short': '{}3/s'.format(units_title),
                'long': 'Cubic {} per Second'.format(units_title_long)
            }
        }

        startdate = qout_data.index[0].strftime('%Y-%m-%dT%H:%M:%SZ')
        enddate = qout_data.index[-1].strftime('%Y-%m-%dT%H:%M:%SZ')
        time_series = []
        for date, value in qout_data.items():
            time_series.append({
                'date': date.strftime('%Y-%m-%dT%H:%M:%SZ'),
                'val': value
            })

        context['startdate'] = startdate
        context['enddate'] = enddate
        context['time_series'] = time_series

        if return_format == "waterml":
            xml_response = make_response(render_template('historic_simulation.xml', **context))
            xml_response.headers.set('Content-Type', 'application/xml')

            return xml_response

        if return_format == "json":
            return jsonify(context)

    else:
        return jsonify({"error": "Invalid return_format."}), 422
