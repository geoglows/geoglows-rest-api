import logging
import traceback

import geoglows
from flask import Blueprint, request, jsonify
from flask_cors import cross_origin

from .analytics import log_request
from .controllers_forecasts import (forecast,
                                    forecast_stats,
                                    forecast_ensemble,
                                    forecast_records,
                                    forecast_dates,
                                    hydroviewer, )
from .controllers_historical import (retrospective_hourly,
                                     retrospective_daily,
                                     retrospective_monthly,
                                     daily_averages,
                                     monthly_averages,
                                     yearly_averages,
                                     return_periods)
from .controllers_misc import get_river_id

logger = logging.getLogger("DEBUG")

app = Blueprint('rest-endpoints-v2', __name__)


@app.route(f'/api/v2/<product>/', methods=['GET'])
@app.route(f'/api/v2/<product>/<river_id>', methods=['GET'])
@cross_origin()
def rest_endpoints_v2(product: str, river_id: int = None):
    product, river_id, return_format, date, start_date, end_date, bias_corrected = handle_request(
        request,
        product,
        river_id,
    )

    log_request(version="v2",
                product=product,
                river_id=river_id,
                return_format=return_format,
                source=request.args.get('source', 'other'), )

    # forecast data products
    if product == 'dates':
        return forecast_dates(return_format=return_format)
    elif product == 'forecast':
        return forecast(river_id, date, return_format=return_format)
    elif product in 'forecaststats':
        return forecast_stats(river_id, date, return_format=return_format)
    elif product == 'forecastensemble':
        return forecast_ensemble(river_id, date, return_format=return_format)
    elif product == 'forecastrecords':
        return forecast_records(river_id, start_date, end_date, return_format=return_format)

    # retrospective data products
    elif product == 'retrospective-daily':
        return retrospective_daily(river_id, return_format=return_format, start_date=start_date, end_date=end_date, bias_corrected=bias_corrected)
    elif product == 'retrospective-hourly':
        return retrospective_hourly(river_id, return_format=return_format, start_date=start_date, end_date=end_date, bias_corrected=bias_corrected)
    elif product == 'retrospective-monthly':
        return retrospective_monthly(river_id, return_format=return_format, start_date=start_date, end_date=end_date, bias_corrected=bias_corrected)
    elif product == 'returnperiods':
        return return_periods(river_id, return_format=return_format, bias_corrected=bias_corrected)
    elif product == 'dailyaverages':
        return daily_averages(river_id, return_format=return_format, bias_corrected=bias_corrected)
    elif product == 'monthlyaverages':
        return monthly_averages(river_id, return_format=return_format, bias_corrected=bias_corrected)
    elif product == 'annualaverages':
        return yearly_averages(river_id, return_format=return_format, bias_corrected=bias_corrected)

    # data availability
    elif product == 'getriverid':
        return get_river_id(request.args.get('lat'), request.args.get('lon'))

    elif product == "hydroviewer":
        return hydroviewer(river_id, date, start_date)

    else:
        return jsonify({'error': f'data product "{product}" not available'}), 201


@app.route(f'/api/v2/log', methods=['POST', ])
def python_package_log_endpoint():
    try:
        data = request.get_json()
        river_id = data.get('river_id', None)
        product = data.get('product', None)
        return_format = data.get('format', 'dataframe')
    except Exception:
        return jsonify({'success': False, 'message': 'invalid parameters'}), 400
    if river_id is None or product is None or return_format is None:
        return jsonify({'success': False, 'message': 'invalid parameters'}), 400
    log_request(
        version="v2",
        product=product,
        river_id=river_id,
        return_format=return_format,
        source='aws-odp',
    )
    return jsonify({'success': True, 'message': 'request logged'}), 200


def handle_request(request, product, river_id):
    ALL_PRODUCTS = {
        'getriverid',

        'dates',
        'forecast',
        'forecaststats',
        'forecastensemble',
        'forecastrecords',

        'retrospective-hourly',
        'retrospective-daily',
        'retrospective-monthly',
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
        'ensembles': 'forecastensemble',
        'ens': 'forecastensemble',
        'records': 'forecastrecords',
        'forecastensembles': 'forecastensemble',

        # aliases for retrospective
        'historical': 'retrospective-daily',
        'historicalsimulation': 'retrospective-daily',
        'hindcast': 'retrospective-daily',
        'historicsimulation': 'retrospective-daily',
        'retrospective': 'retrospective-daily',

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

    if product in ('dates', 'getriverid'):
        # dates & getriverid do not apply to a single ID
        river_id = None
    elif river_id is None:  # all other products require an ID - try to find it from the lat/lon
        if request.args.get('lat', None) and request.args.get('lon', None):
            try:
                river_id = geoglows.streams.latlon_to_river(
                    float(request.args.get('lat')), float(request.args.get('lon'))
                )
            except Exception:
                raise ValueError('invalid lat/lon provided')
        else:
            raise ValueError('you must specify a river ID number for this dataset')
    elif river_id is not None:  # otherwise do a simple check that the river_id might be valid
        try:
            river_id = str(river_id).replace(' ', '').replace('_', '').replace('-', '')
            river_id = int(river_id)
            assert river_id > 110_000_000
        except Exception:
            raise ValueError("river_id must be a 9 digit integer of a valid river ID")

    return_format = request.args.get('format', 'csv')
    if return_format not in return_formats:
        raise ValueError('format not recognized. must be either "json" or "csv"')

    date = request.args.get('date', 'latest')
    start_date = request.args.get('start_date', None)
    end_date = request.args.get('end_date', None)
    bias_corrected = request.args.get('bias_corrected', 'false').lower() in ['true']

    return (
        product,
        river_id,
        return_format,
        date,
        start_date,
        end_date,
        bias_corrected
    )


@app.errorhandler(ValueError)
def errors_value_error(e: ValueError):
    logger.debug(traceback.format_exc())
    return jsonify({"error": f"Invalid request: {e}"}), 400


@app.errorhandler(Exception)
def errors_general_exception(e: Exception):
    logger.debug(traceback.format_exc())
    return jsonify({"error": f"An unexpected error occurred: {e}"}), 500
