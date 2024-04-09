import logging
import traceback

import geoglows
from flask import Blueprint, request, jsonify
from flask_cors import cross_origin

from .analytics import log_request
from .constants import PACKAGE_METADATA_TABLE_PATH
from .controllers_forecasts import (forecast,
                                    forecast_stats,
                                    forecast_ensembles,
                                    forecast_records,
                                    forecast_dates,
                                    hydroviewer, )
from .controllers_historical import (retrospective,
                                     daily_averages,
                                     monthly_averages,
                                     yearly_averages,
                                     return_periods)
from .controllers_misc import get_reach_id

logger = logging.getLogger("DEBUG")

app = Blueprint('rest-endpoints-v2', __name__)

geoglows.METADATA_TABLE_PATH = PACKAGE_METADATA_TABLE_PATH


@app.route(f'/api/v2/<product>/', methods=['GET'])
@app.route(f'/api/v2/<product>/<reach_id>', methods=['GET'])
@cross_origin()
def rest_endpoints_v2(product: str, reach_id: int = None):
    product, reach_id, return_format, date, ensemble, start_date, end_date = handle_request(
        request,
        product,
        reach_id,
    )

    log_request(version="v2",
                product=product,
                reach_id=reach_id,
                return_format=return_format,
                source=request.args.get('source', 'other'), )

    # forecast data products
    if product == 'dates':
        return forecast_dates(return_format=return_format)
    elif product == 'forecast':
        return forecast(reach_id, date, return_format=return_format)
    elif product in 'forecaststats':
        return forecast_stats(reach_id, date, return_format=return_format)
    elif product == 'forecastensembles':
        return forecast_ensembles(reach_id, date, return_format=return_format, ensemble=ensemble)
    elif product == 'forecastrecords':
        return forecast_records(reach_id, start_date, end_date, return_format=return_format)

    # hindcast data products
    elif product == 'retrospective':
        return retrospective(reach_id, return_format=return_format, start_date=start_date, end_date=end_date)
    elif product == 'returnperiods':
        return return_periods(reach_id, return_format=return_format)
    elif product == 'dailyaverages':
        return daily_averages(reach_id, return_format=return_format)
    elif product == 'monthlyaverages':
        return monthly_averages(reach_id, return_format=return_format)
    elif product == 'annualaverages':
        return yearly_averages(reach_id, return_format=return_format)

    # data availability
    elif product == 'getreachid':
        return get_reach_id(request.args.get('lat'), request.args.get('lon'))

    elif product == "hydroviewer":
        return hydroviewer(reach_id, start_date, date)

    else:
        return jsonify({'error': f'data product "{product}" not available'}), 201


def handle_request(request, product, reach_id):
    ALL_PRODUCTS = {
        'getreachid',

        'dates',
        'forecast',
        'forecaststats',
        'forecastensembles',
        'forecastrecords',

        'retrospective',
        'monthlyaverages',
        'dailyaverages',
        'annualaverages',
        'returnperiods',

        'hydroviewer',
    }

    # Recognized shorthand names and their proper product name
    PRODUCT_SHORTCUTS = {
        'availabledates': 'dates',
        'forecastdates': 'dates',

        # forecast products
        'stats': 'forecaststats',
        'ensembles': 'forecastensembles',
        'ens': 'forecastensembles',
        'records': 'forecastrecords',

        # aliases for retrospective
        'historical': 'retrospective',
        'historicalsimulation': 'retrospective',
        'hindcast': 'retrospective',
        'historicsimulation': 'retrospective',

        # aliases for derived historical products
        'monavg': 'monthlyaverages',
        'dayavg': 'dailyaverages',
        'yearavg': 'annualaverages',
        'yearlyaverages': 'annualaverages',
    }
    return_formats = ('csv', 'json',)

    product = str(product).lower().replace(' ', '').replace('_', '').replace('-', '')
    if product not in ALL_PRODUCTS:
        if product not in PRODUCT_SHORTCUTS.keys():
            raise ValueError(f'{product} not recognized. available products: {ALL_PRODUCTS}')
        product = PRODUCT_SHORTCUTS[product]

    if product in ('dates', 'getreachid'):
        # dates & getreachid do not apply to a single ID
        reach_id = None
    elif reach_id is None:  # all other products require an ID - try to find it from the lat/lon
        if request.args.get('lat', None) and request.args.get('lon', None):
            reach_id = geoglows.streams.latlon_to_reach(float(request.args.get('lat')), float(request.args.get('lon')))
        else:
            raise ValueError('you must specify a river ID number for this dataset')
    elif reach_id is not None:  # otherwise do a simple check that the reach_id might be valid
        try:
            reach_id = str(reach_id).replace(' ', '').replace('_', '').replace('-', '')
            reach_id = int(reach_id)
            assert reach_id > 110_000_000
        except Exception:
            raise ValueError("reach_id must be a 9 digit integer of a valid river ID")

    return_format = request.args.get('format', 'csv')
    if return_format not in return_formats:
        raise ValueError('format not recognized. must be either "json" or "csv"')

    date = request.args.get('date', 'latest')
    start_date = request.args.get('start_date', None)
    end_date = request.args.get('end_date', None)

    ensemble = request.args.get('ensemble', 'all')

    return (
        product,
        reach_id,
        return_format,
        date,
        ensemble,
        start_date,
        end_date
    )


@app.errorhandler(ValueError)
def errors_value_error(e: ValueError):
    logger.debug(traceback.format_exc())
    return jsonify({"error": f"Invalid request: {e}"}), 400


@app.errorhandler(Exception)
def errors_general_exception(e: Exception):
    logger.debug(traceback.format_exc())
    return jsonify({"error": f"An unexpected error occurred: {e}"}), 500
