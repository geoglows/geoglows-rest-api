from main_controller import get_forecast_streamflow_csv, get_ecmwf_forecast_statistics
from functions import get_units_title

from flask import render_template
import logging


# create logger function
def init_logger():
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    handler = logging.FileHandler('/app/api.log', 'a')
    formatter = logging.Formatter('%(asctime)s: %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    
def get_ecmwf_forecast(request):
    """
    Controller that will retrieve the ECMWF forecast data
    in WaterML 1.1 or CSV format
    """
    
    init_logger()
    return_format = request['return_format']

    if return_format == 'csv':
        return get_forecast_streamflow_csv(request)

    # return WaterML
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
    units_title_long = 'meters'
    if units_title == 'ft':
        units_title_long = 'feet'

    try:
        stat = request['stat']
    except KeyError:
        logging.error('Missing value for stat_type ...')

    if stat not in formatted_stat:
        logging.error('Invalid value for stat_type ...')

    startdate = forecast_statistics[stat].index[0]\
                                         .strftime('%Y-%m-%d %H:%M:%S')
    time_series = []
    for date, value in forecast_statistics[stat].iteritems():
        time_series.append({
            'date': date.strftime('%Y-%m-%dT%H:%M:%S'),
            'val': value
        })

    content = {
        'config': watershed_name,
        'comid': river_id,
        'stat': formatted_stat[stat],
        'startdate': startdate,
        'site_name': watershed_name + ' ' + subbasin_name,
        'units': {
            'name': 'Flow',
            'short': '{}^3/s'.format(units_title),
            'long': 'Cubic {} per Second'.format(units_title_long)
        },
        'time_series': time_series,
        'Source': 'ECMWF GloFAS forecast',
        'host': 'https://%s' % request.get_host(),
    }

    xml_response = \
        render_template('waterml.xml', **content)
    xml_response.headers['Content-Type'] = 'application/xml'

    return xml_response
