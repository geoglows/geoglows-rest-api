from flask import Blueprint, request, jsonify
from flask_cors import cross_origin

from .analytics import log_request
from .v2_controllers_forecasts import (forecast,
                                       forecast_stats,
                                       forecast_ensembles,
                                       forecast_records,
                                       forecast_anomalies,
                                       forecast_warnings,
                                       forecast_dates,
                                       hydroviewer)
from .v2_controllers_historical import (historical,
                                        historical_averages,
                                        return_periods)
from .v2_utilities import handle_request

app = Blueprint('rest-endpoints-v2', __name__)


@app.route(f'/api/latest/<product>/', methods=['GET'])
@app.route(f'/api/latest/<product>/<reach_id>', methods=['GET'])
@app.route(f'/api/v2/<product>/', methods=['GET'])
@app.route(f'/api/v2/<product>/<reach_id>', methods=['GET'])
@cross_origin()
def rest_endpoints_v2(product: str, reach_id: int = None):
    product, reach_id, return_format, units, date, ensemble, start_date, end_date = handle_request(
        request,
        product,
        reach_id,
    )

    log_request(version="v2",
                product=product,
                reach_id=request.args.get('reach_id', 0),
                return_format=return_format,
                source=request.args.get('source', 'other'), )

    # forecast data products
    if product == 'forecast':
        return forecast(reach_id, date, units, return_format)
    elif product in 'forecaststats':
        return forecast_stats(reach_id, date, units, return_format)
    elif product == 'forecastensembles':
        return forecast_ensembles(reach_id, date, units, return_format, ensemble)
    elif product == 'forecastrecords':
        return forecast_records(reach_id, start_date, end_date, units, return_format)
    elif product == 'forecastanomalies':
        return forecast_anomalies(reach_id, date, units, return_format)
    elif product == 'forecastwarnings':
        return forecast_warnings(date, return_format)
    elif product == 'forecastdates':
        return forecast_dates()
    if product == "hydroviewer":
        return hydroviewer(reach_id, start_date, date, units, return_format)

    # hindcast data products
    elif product == 'hindcast':
        return historical(reach_id, units, return_format)
    elif product == 'returnperiods':
        return return_periods(reach_id, units, return_format)
    elif product == 'dailyaverages':
        return historical_averages(reach_id, units, 'daily', return_format)
    elif product == 'monthlyaverages':
        return historical_averages(reach_id, units, 'monthly', return_format)

    # data availability
    # todo
    # elif product == 'getreachid':
    #     return v1_controllers.get_reach_id_from_latlon_handler(request)

    else:
        return jsonify({'status': f'data product "{product}" not available'}), 201
