import logging
import os
from datetime import datetime as dt
import xarray
import pandas as pd

from flask import jsonify, render_template, make_response
from functions import get_units_title, reach_to_region
from main_controller import (get_forecast_streamflow_csv,
                             get_ecmwf_forecast_statistics,
                             get_forecast_warnings,
                             get_forecast_ensemble_csv,
                             get_ecmwf_ensemble,
                             get_historic_data_csv,
                             get_historic_streamflow_series,
                             get_seasonal_avg_csv,
                             get_seasonal_average,
                             get_return_period_csv,
                             get_return_period_dict,
                             get_reach_from_latlon)

# GLOBAL
PATH_TO_ERA_INTERIM = '/mnt/output/era-interim'
PATH_TO_ERA_5 = '/mnt/output/era-5'
PATH_TO_FORECASTS = '/mnt/output/forecasts'
PATH_TO_FORECAST_RECORDS = '/mnt/output/forecast-records'


# create logger function
def init_logger():
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    handler = logging.FileHandler('/app/api.log', 'a')
    formatter = logging.Formatter('%(asctime)s: %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)


def forecast_stats_handler(request):
    """
    Controller that will retrieve forecast statistic data
    in different formats
    """
    init_logger()

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
            'gendate': dt.utcnow().isoformat() + 'Z',
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


def forecast_ensembles_handler(request):
    """
    Controller that will retrieve forecast ensemble data
    in different formats
    """
    init_logger()

    return_format = request.args.get('return_format', 'csv')

    if return_format in ('csv', ''):
        csv_response = get_forecast_ensemble_csv(request)
        if isinstance(csv_response, dict) and "error" in csv_response.keys():
            return jsonify(csv_response)
        else:
            return csv_response

    elif return_format in ('waterml', 'json'):

        # retrieve statistics
        forecast_ensembles, region, reach_id, units = get_ecmwf_ensemble(request)

        formatted_ensemble = {}
        for ens in sorted(forecast_ensembles.keys()):
            formatted_ensemble[ens.split('_')[1]] = ens.title().replace('_', ' ')

        units_title = get_units_title(units)
        units_title_long = 'Meters'
        if units_title == 'ft':
            units_title_long = 'Feet'

        ensemble = request.args.get('ensemble', 'all')

        context = {
            'region': region,
            'comid': reach_id,
            'gendate': dt.utcnow().isoformat() + 'Z',
            'units': {
                'name': 'Streamflow',
                'short': '{}3/s'.format(units_title),
                'long': 'Cubic {} per Second'.format(units_title_long)
            }
        }

        ensemble_ts_dict = {}
        if ensemble != '' and ensemble != 'all' and '-' not in ensemble and ',' not in ensemble:

            if int(ensemble) not in map(int, sorted(formatted_ensemble.keys())):
                logging.error('Invalid value for ensemble ...')
                return jsonify({"error": "Invalid value for ensemble parameter."}), 422

            startdate = forecast_ensembles['ensemble_{0:02}'.format(int(ensemble))].index[0] \
                .strftime('%Y-%m-%dT%H:%M:%SZ')
            enddate = forecast_ensembles['ensemble_{0:02}'.format(int(ensemble))].index[-1] \
                .strftime('%Y-%m-%dT%H:%M:%SZ')

            time_series = []
            for date, value in forecast_ensembles['ensemble_{0:02}'.format(int(ensemble))].items():
                time_series.append({
                    'date': date.strftime('%Y-%m-%dT%H:%M:%SZ'),
                    'val': value
                })

            context['ensembles'] = {'{0:02}'.format(int(ensemble)): formatted_ensemble['{0:02}'.format(int(ensemble))]}
            context['startdate'] = startdate
            context['enddate'] = enddate
            ensemble_ts_dict['{0:02}'.format(int(ensemble))] = time_series

        elif ensemble in ('', 'all'):

            startdate = forecast_ensembles['ensemble_01'].index[0].strftime('%Y-%m-%dT%H:%M:%SZ')
            enddate = forecast_ensembles['ensemble_01'].index[-1].strftime('%Y-%m-%dT%H:%M:%SZ')

            for i in range(1, 53):
                ens_time_series = []

                for date, value in forecast_ensembles['ensemble_{0:02}'.format(i)].items():
                    ens_time_series.append({
                        'date': date.strftime('%Y-%m-%dT%H:%M:%SZ'),
                        'val': value
                    })

                ensemble_ts_dict['{0:02}'.format(i)] = ens_time_series

            context['ensembles'] = formatted_ensemble
            context['startdate'] = startdate
            context['enddate'] = enddate

        elif ensemble != '' and ensemble != 'all' and '-' in ensemble and ',' not in ensemble:

            if ensemble.split('-')[0] == '':
                start = 1
            else:
                start = int(ensemble.split('-')[0])

            if ensemble.split('-')[1] == '':
                stop = 53
            else:
                stop = int(ensemble.split('-')[1]) + 1

            if start > 53:
                start = 1
            if stop > 53:
                stop = 53

            startdate = forecast_ensembles['ensemble_{0:02}'.format(start)].index[0] \
                .strftime('%Y-%m-%dT%H:%M:%SZ')
            enddate = forecast_ensembles['ensemble_{0:02}'.format(start)].index[-1] \
                .strftime('%Y-%m-%dT%H:%M:%SZ')

            for i in range(start, stop):
                ens_time_series = []

                for date, value in forecast_ensembles['ensemble_{0:02}'.format(i)].items():
                    ens_time_series.append({
                        'date': date.strftime('%Y-%m-%dT%H:%M:%SZ'),
                        'val': value
                    })

                ensemble_ts_dict['{0:02}'.format(i)] = ens_time_series

            context['ensembles'] = formatted_ensemble
            context['startdate'] = startdate
            context['enddate'] = enddate

        elif ensemble != '' and ensemble != 'all' and '-' not in ensemble and ',' in ensemble:

            ens_list = list(map(int, ensemble.replace(' ', '').split(',')))

            startdate = forecast_ensembles['ensemble_{0:02}'.format(ens_list[0])].index[0] \
                .strftime('%Y-%m-%dT%H:%M:%SZ')
            enddate = forecast_ensembles['ensemble_{0:02}'.format(ens_list[0])].index[-1] \
                .strftime('%Y-%m-%dT%H:%M:%SZ')

            for i in ens_list:
                ens_time_series = []

                for date, value in forecast_ensembles['ensemble_{0:02}'.format(i)].items():
                    ens_time_series.append({
                        'date': date.strftime('%Y-%m-%dT%H:%M:%SZ'),
                        'val': value
                    })

                ensemble_ts_dict['{0:02}'.format(i)] = ens_time_series

            context['ensembles'] = formatted_ensemble
            context['startdate'] = startdate
            context['enddate'] = enddate

        context['time_series'] = ensemble_ts_dict

        if return_format == "waterml":
            xml_response = make_response(render_template('forecast_ensembles.xml', **context))
            xml_response.headers.set('Content-Type', 'application/xml')

            return xml_response

        if return_format == "json":
            return jsonify(context)

    else:
        return jsonify({"error": "Invalid return_format."}), 422


def forecast_warnings_handler(request):
    region = request.args.get('region', False)
    lat = request.args.get('lat', False)
    lon = request.args.get('lon', False)
    forecast_date = request.args.get('forecast_date', 'most_recent')

    try:
        print('made it to the handler')
        csv_response = get_forecast_warnings(region, lat, lon, forecast_date)
        if isinstance(csv_response, dict) and "error" in csv_response.keys():
            return jsonify(csv_response)
        else:
            print('in the else')
            return csv_response
    except Exception as e:
        print(e)
        return jsonify({"error": e}), 422


def forecast_records_handler(request):
    reach_id = int(request.args.get('reach_id', False))
    lat = request.args.get('lat', '')
    lon = request.args.get('lon', '')
    return_format = request.args.get('return_format', 'csv')

    year = dt.utcnow().year
    start_date = request.args.get('start_date', dt(year=year, month=1, day=1).strftime('%Y%m%d'))
    end_date = request.args.get('end_date', dt(year=year, month=12, day=31).strftime('%Y%m%d'))

    # determine if you have a reach_id and region from the inputs
    if reach_id:
        region = reach_to_region(reach_id)
        if not region:
            return jsonify({"error": "Unable to determine a region paired with this reach_id"}, 422)
    elif lat != '' and lon != '':
        reach_id, region, dist_error = get_reach_from_latlon(lat, lon)
        if dist_error:
            return jsonify(dist_error)
    else:
        return jsonify({"error": "Invalid reach_id or lat/lon/region combination"}, 422)

    # validate the times
    try:
        start_date = dt.strptime(start_date, '%Y%m%d')
        end_date = dt.strptime(end_date, '%Y%m%d')
    except:
        return jsonify({'Error': 'Unrecognized start_date or end_date. Use YYYYMMDD format'})

    # open and read the forecast record netcdf
    record_path = os.path.join(PATH_TO_FORECAST_RECORDS, region, 'forecast_record-{0}-{1}.nc'.format(year, region))
    forecast_record = xarray.open_dataset(record_path)
    times = pd.to_datetime(pd.Series(forecast_record['time'].data, name='datetime'), unit='s', origin='unix')
    record_flows = forecast_record.sel(rivid=reach_id)['Qout']
    forecast_record.close()

    # create a dataframe and filter by date
    flow_series_df = times.to_frame().join(pd.Series(record_flows, name='streamflow (m^3/s)'))
    flow_series_df = flow_series_df[flow_series_df['datetime'].between(start_date, end_date)]
    flow_series_df[flow_series_df['streamflow (m^3/s)'] > 1000000000] = np.nan
    flow_series_df.dropna(inplace=True)

    # create the http response
    response = make_response(flow_series_df.to_csv(index=False))
    response.headers['content-type'] = 'text/csv'
    response.headers['Content-Disposition'] = 'attachment; filename=forecast_record_{0}.csv'.format(reach_id)
    return response


def historic_data_handler(request):
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
            'gendate': dt.utcnow().isoformat() + 'Z',
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


def seasonal_average_handler(request):
    """
    Controller for retrieving seasonal averages
    """
    return_format = request.args.get('return_format', 'csv')

    if return_format == 'csv':
        csv_response = get_seasonal_avg_csv(request)
        if isinstance(csv_response, dict) and "error" in csv_response.keys():
            return jsonify(csv_response)
        else:
            return csv_response

    elif return_format == 'waterml' or return_format == 'json':

        qout_data, region, reach_id, units = get_seasonal_average(request)

        units_title = get_units_title(units)
        units_title_long = 'meters'
        if units_title == 'ft':
            units_title_long = 'feet'

        context = {
            'region': region,
            'comid': reach_id,
            'gendate': dt.utcnow().isoformat() + 'Z',
            'units': {
                'name': 'Streamflow',
                'short': '{}3/s'.format(units_title),
                'long': 'Cubic {} per Second'.format(units_title_long)
            }
        }

        time_series = []
        for day, value in qout_data.items():
            time_series.append({
                'day': day,
                'val': value
            })

        context['time_series'] = time_series

        if return_format == "waterml":
            xml_response = \
                make_response(render_template('seasonal_averages.xml', **context))
            xml_response.headers.set('Content-Type', 'application/xml')

            return xml_response

        if return_format == "json":
            return jsonify(context)

    else:
        return jsonify({"error": "Invalid return_format."}), 422


def return_periods_handler(request):
    """
    Controller that will show the historic data in WaterML 1.1 format
    """
    return_format = request.args.get('return_format', 'csv')

    if return_format in ('csv', ''):
        csv_response = get_return_period_csv(request)
        if isinstance(csv_response, dict) and "error" in csv_response.keys():
            return jsonify(csv_response)
        else:
            return csv_response

    elif return_format in ('waterml', 'json'):

        return_period_data, region, reach_id, units = get_return_period_dict(request)

        units_title = get_units_title(units)
        units_title_long = 'meters'
        if units_title == 'ft':
            units_title_long = 'feet'

        context = {
            'region': region,
            'comid': reach_id,
            'gendate': dt.utcnow().isoformat() + 'Z',
            'units': {
                'name': 'Streamflow',
                'short': '{}3/s'.format(units_title),
                'long': 'Cubic {} per Second'.format(units_title_long)
            }
        }

        startdate = '1980-01-01T00:00:00Z'
        enddate = '2014-12-31T00:00:00Z'
        time_series = []
        for period, value in return_period_data.items():
            time_series.append({
                'period': period,
                'val': value
            })

        context['startdate'] = startdate
        context['enddate'] = enddate
        context['time_series'] = time_series

        if return_format == "waterml":
            xml_response = \
                make_response(render_template('return_periods.xml', **context))
            xml_response.headers.set('Content-Type', 'application/xml')

            return xml_response

        if return_format == "json":
            return jsonify(context)

    else:
        return jsonify({"error": "Invalid return_format."}), 422


def available_data_handler():
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


def get_dates_handler(request):
    """
    Controller that returns available dates.
    """
    region = request.args.get('region', '')

    region_path = os.path.join(PATH_TO_FORECASTS, region)

    if not os.path.exists(region_path):
        return jsonify({"message": "Region does not exist."})

    dates = [d for d in os.listdir(region_path) if d.split('.')[0].isdigit()]

    if len(dates) > 0:
        return jsonify({"available_dates": dates})
    else:
        return jsonify({"message": "No dates available."}), 204


def get_reach_id_from_latlon_handler(request):
    """
    Controller that returns the reach_id nearest to valid lat/lon coordinates
    """
    lat = request.args.get('lat', '')
    lon = request.args.get('lon', '')
    return jsonify(get_reach_from_latlon(lat, lon))
