from main_controller import (get_forecast_streamflow_csv, get_ecmwf_forecast_statistics,
                             get_forecast_ensemble_csv, get_ecmwf_ensemble,
                             get_historic_data_csv, get_historic_streamflow_series,
                             get_return_period_csv, get_return_period_dict)
                            
from functions import get_units_title

from datetime import datetime as dt
from flask import jsonify, render_template, make_response

import logging
import os


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
    
    return_format = request.args.get('return_format', '')

    if (return_format == 'csv' or return_format == ''):
        csv_response = get_forecast_streamflow_csv(request)
        if (isinstance(csv_response, dict) and "error" in csv_response.keys()):
            return jsonify(csv_response)
        else:
            return csv_response
    
    elif (return_format == 'waterml' or return_format == 'json'):

        formatted_stat = {
            'high_res': 'High Resolution',
            'mean': 'Mean',
            'min': 'Min',
            'max': 'Max',
            'std_dev_range_lower': 'Standard Deviation Lower Range',
            'std_dev_range_upper': 'Standard Deviation Upper Range',
        }
    
        # retrieve statistics
        forecast_statistics, watershed_name, subbasin_name, river_id, units = \
            get_ecmwf_forecast_statistics(request)
    
        units_title = get_units_title(units)
        units_title_long = 'Meters'
        if units_title == 'ft':
            units_title_long = 'Feet'
    
        stat = request.args.get('stat', '')
    
        context = {
            'region': "-".join([watershed_name, subbasin_name]),
            'comid': river_id,
            'gendate': dt.utcnow().isoformat() + 'Z',
            'units': {
                'name': 'Streamflow',
                'short': '{}3/s'.format(units_title),
                'long': 'Cubic {} per Second'.format(units_title_long)
            }
        }
    
        stat_ts_dict = {}
        if (stat != '' and stat != 'all'):
    
            if stat not in formatted_stat.keys():
                logging.error('Invalid value for stat ...')
                return jsonify({"error": "Invalid value for stat parameter."})
                
            startdate = forecast_statistics[stat].index[0]\
                .strftime('%Y-%m-%dT%H:%M:%SZ')
            enddate = forecast_statistics[stat].index[-1]\
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
            startdate = forecast_statistics['mean'].index[0]\
                .strftime('%Y-%m-%dT%H:%M:%SZ')
            enddate = forecast_statistics['mean'].index[-1]\
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
        return jsonify({"error": "Invalid return_format."})


def forecast_ensembles_handler(request):
    """
    Controller that will retrieve forecast ensemble data
    in different formats
    """
    
    init_logger()
    
    return_format = request.args.get('return_format', '')

    if (return_format == 'csv' or return_format == ''):
        csv_response = get_forecast_ensemble_csv(request)
        if (isinstance(csv_response, dict) and "error" in csv_response.keys()):
            return jsonify(csv_response)
        else:
            return csv_response
    
    elif (return_format == 'waterml' or return_format == 'json'):
    
        # retrieve statistics
        forecast_ensembles, watershed_name, subbasin_name, river_id, units = \
            get_ecmwf_ensemble(request)

        formatted_ensemble = {}
        for ens in sorted(forecast_ensembles.keys()):
            formatted_ensemble[ens.split('_')[1]] = ens.title().replace('_', ' ')

        units_title = get_units_title(units)
        units_title_long = 'Meters'
        if units_title == 'ft':
            units_title_long = 'Feet'
    
        ensemble = request.args.get('ensemble', '')
    
        context = {
            'region': "-".join([watershed_name, subbasin_name]),
            'comid': river_id,
            'gendate': dt.utcnow().isoformat() + 'Z',
            'units': {
                'name': 'Streamflow',
                'short': '{}3/s'.format(units_title),
                'long': 'Cubic {} per Second'.format(units_title_long)
            }
        }
    
        ensemble_ts_dict = {}
        if (ensemble != '' and ensemble != 'all' and '-' not in ensemble and ',' not in ensemble):
    
            if int(ensemble) not in map(int, sorted(formatted_ensemble.keys())):
                logging.error('Invalid value for ensemble ...')
                return jsonify({"error": "Invalid value for ensemble parameter."})

            startdate = forecast_ensembles['ensemble_{0:02}'.format(int(ensemble))].index[0]\
                .strftime('%Y-%m-%dT%H:%M:%SZ')
            enddate = forecast_ensembles['ensemble_{0:02}'.format(int(ensemble))].index[-1]\
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
            
        elif ((ensemble == '' or ensemble == 'all') and '-' not in ensemble and ',' not in ensemble):

            startdate = forecast_ensembles['ensemble_01'].index[0]\
                .strftime('%Y-%m-%dT%H:%M:%SZ')
            enddate = forecast_ensembles['ensemble_01'].index[-1]\
                .strftime('%Y-%m-%dT%H:%M:%SZ')
            
            for i in range(1,53):
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
        
        elif (ensemble != '' and ensemble != 'all' and '-' in ensemble and ',' not in ensemble):

            
            if ensemble.split('-')[0] == '':
                start = 1
            else:
                start = int(ensemble.split('-')[0])
            
            if ensemble.split('-')[1] == '':
                stop = 53
            else:
                stop = int(ensemble.split('-')[1])+1

            if start > 53:
                start = 1
            if stop > 53:
                stop = 53

            startdate = forecast_ensembles['ensemble_{0:02}'.format(start)].index[0]\
                .strftime('%Y-%m-%dT%H:%M:%SZ')
            enddate = forecast_ensembles['ensemble_{0:02}'.format(start)].index[-1]\
                .strftime('%Y-%m-%dT%H:%M:%SZ')
                
            for i in range(start,stop):
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
            
        elif (ensemble != '' and ensemble != 'all' and '-' not in ensemble and ',' in ensemble):

            ens_list = list(map(int, ensemble.replace(' ', '').split(',')))

            startdate = forecast_ensembles['ensemble_{0:02}'.format(ens_list[0])].index[0]\
                .strftime('%Y-%m-%dT%H:%M:%SZ')
            enddate = forecast_ensembles['ensemble_{0:02}'.format(ens_list[0])].index[-1]\
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
            xml_response = \
                make_response(render_template('forecast_ensembles.xml', **context))
            xml_response.headers.set('Content-Type', 'application/xml')
        
            return xml_response
        
        if return_format == "json":
            return jsonify(context)
        
    else:
        return jsonify({"error": "Invalid return_format."})


def historic_data_handler(request):
    """
    Controller that will show the historic data in WaterML 1.1 format
    """
    return_format = request.args.get('return_format', '')

    if (return_format == 'csv' or return_format == ''):
        csv_response = get_historic_data_csv(request)
        if (isinstance(csv_response, dict) and "error" in csv_response.keys()):
            return jsonify(csv_response)
        else:
            return csv_response
        
    elif (return_format == 'waterml' or return_format == 'json'):

        qout_data, river_id, watershed_name, subbasin_name, units =\
            get_historic_streamflow_series(request)
    
        units_title = get_units_title(units)
        units_title_long = 'meters'
        if units_title == 'ft':
            units_title_long = 'feet'
            
        context = {
            'region': "-".join([watershed_name, subbasin_name]),
            'comid': river_id,
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
            xml_response = \
                make_response(render_template('historic_simulation.xml', **context))
            xml_response.headers.set('Content-Type', 'application/xml')
        
            return xml_response
        
        if return_format == "json":
            return jsonify(context)
        
    else:
        return jsonify({"error": "Invalid return_format."})


def return_periods_handler(request):
    """
    Controller that will show the historic data in WaterML 1.1 format
    """
    return_format = request.args.get('return_format', '')

    if (return_format == 'csv' or return_format == ''):
        csv_response = get_return_period_csv(request)
        if (isinstance(csv_response, dict) and "error" in csv_response.keys()):
            return jsonify(csv_response)
        else:
            return csv_response
        
    elif (return_format == 'waterml' or return_format == 'json'):

        return_period_data, river_id, watershed_name, subbasin_name, units =\
            get_return_period_dict(request)
    
        units_title = get_units_title(units)
        units_title_long = 'meters'
        if units_title == 'ft':
            units_title_long = 'feet'
            
        context = {
            'region': "-".join([watershed_name, subbasin_name]),
            'comid': river_id,
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
        return jsonify({"error": "Invalid return_format."})
    
    
def get_region_handler():
    """
    Controller that returns available regions.
    """
    path_to_rapid_output = "/mnt/output/ecmwf"

    regions = os.listdir(path_to_rapid_output)

    if len(regions) > 0:
        return jsonify({"available_regions": regions})
    else:
        return jsonify({"message": "No regions found."})


def get_available_dates_handler(request):
    """
    Controller that returns available dates.
    """
    path_to_rapid_output = "/mnt/output/ecmwf"
    
    region = request.args.get('region', '')
    
    region_path = os.path.join(path_to_rapid_output, region)
    
    if not os.path.exists(region_path):
        return jsonify({"message": "Region does not exist."})

    dates = os.listdir(region_path)

    if len(dates) > 0:
        return jsonify({"available_dates": dates})
    else:
        return jsonify({"message": "No dates available."})
    