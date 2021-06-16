import logging
from os import getenv

from flask import Flask, render_template, request, jsonify, url_for, redirect, make_response
from flask_cors import CORS, cross_origin

import v1_controllers
import v2_controllers
import water_one_flow

print("Creating Application")

api_path = getenv('API_PREFIX')
wof_path = '/wof'

app = Flask(__name__)
app.url_map.strict_slashes = False
app.debug = False

cors = CORS(app)
app.config['CORS_HEADERS'] = '*'


# create logger function
def init_logger():
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    handler = logging.FileHandler('/app/api.log', 'a')
    formatter = logging.Formatter('%(asctime)s: %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)


# >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> HTML PAGES
@app.route('/')
@cross_origin()
def home():
    return render_template('home.html')


@app.route('/documentation')
@cross_origin()
def documentation():
    return render_template('documentation.html')


@app.route('/publications')
@cross_origin()
def publications():
    return render_template('publications.html')


@app.route('/about')
@cross_origin()
def about():
    return render_template('about.html')


@app.route('/resources')
@cross_origin()
def resources():
    return render_template('resources.html')


# >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> REST API ENDPOINTS
# @app.route(f'{api_path}', methods=['GET'])
# @cross_origin()
# def rest_api():
#     return


@app.route(f'{api_path}/latest/<product>/', methods=['GET'], )
@app.route(f'{api_path}/v2/<product>/', methods=['GET'])
@app.route(f'{api_path}/latest/<product>/<reach_id>', methods=['GET'], )
@app.route(f'{api_path}/v2/<product>/<reach_id>', methods=['GET'])
@cross_origin()
def rest_endpoints_v2(product: str, reach_id: int = None):
    product, reach_id, units, return_format, date, ensemble, start_date, end_date = \
        v2_controllers.handle_request(request, product, reach_id)

    # forecast data products
    if product == 'forecast':
        return v2_controllers.forecast(reach_id, date, units, return_format)
    elif product == 'forecaststats':
        return v2_controllers.forecast_stats(reach_id, date, units, return_format)
    elif product == 'forecastensembles':
        return v2_controllers.forecast_ensembles(reach_id, date, units, return_format, ensemble)
    elif product == 'forecastwarnings':
        return v2_controllers.forecast_warnings(request)
    elif product == 'forecastrecords':
        return v2_controllers.forecast_records(reach_id, start_date, end_date, units, return_format)
    elif product == 'forecastanomalies':
        return v2_controllers.forecast_anomalies(reach_id, date, units, return_format)

    # historical data products
    elif product == 'historical':
        return v2_controllers.historical(reach_id, units, return_format)
    elif product == 'returnperiods':
        return v2_controllers.return_periods(reach_id, units, return_format)
    elif product == 'dailyaverages':
        return v2_controllers.historical_averages(request, units, 'daily', return_format)
    elif product == 'monthlyaverages':
        return v2_controllers.historical_averages(request, units, 'monthly', return_format)

    # data availability
    elif product == 'AvailableData':
        return v1_controllers.get_available_data_handler()
    elif product == 'AvailableRegions':
        return v1_controllers.get_region_handler()
    elif product == 'AvailableDates':
        return v1_controllers.available_dates(request)
    elif product == 'GetReachID':
        return v1_controllers.get_reach_id_from_latlon_handler(request)

    else:
        return jsonify({"status": "success"})


@app.route(f'{api_path}/<product>', methods=['GET'], )
@app.route(f'{api_path}/v1/<product>', methods=['GET'])
@cross_origin()
def rest_endpoints_v1(product: str):
    # forecast data products
    if product == 'ForecastStats':
        return v1_controllers.forecast_stats(request)
    elif product == 'ForecastEnsembles':
        return v1_controllers.forecast_ensembles(request)
    elif product == 'ForecastWarnings':
        return v1_controllers.forecast_warnings(request)
    elif product == 'ForecastRecords':
        return v1_controllers.forecast_records(request)

    # historical data products
    elif product == 'HistoricSimulation':
        return v1_controllers.historical(request)
    elif product == 'ReturnPeriods':
        return v1_controllers.return_periods(request)
    elif product == 'DailyAverages':
        return v1_controllers.historical_averages(request, 'daily')
    elif product == 'MonthlyAverages':
        return v1_controllers.historical_averages(request, 'monthly')

    elif product == 'AvailableData':
        return v1_controllers.get_available_data_handler()
    elif product == 'AvailableRegions':
        return v1_controllers.get_region_handler()
    elif product == 'AvailableDates':
        return v1_controllers.available_dates(request)
    elif product == 'GetReachID':
        return v1_controllers.get_reach_id_from_latlon_handler(request)

    else:
        return jsonify({"status": "success"})


# >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> WATERONEFLOW ENDPOINTS
@app.route(f'{wof_path}', methods=['GET'])
@app.route(f'{wof_path}/<product>', methods=['GET'])
@cross_origin()
def wof_endpoints(product: str = 'WSDL'):
    if product == 'WSDL':
        ...
    elif product == 'GetSites':
        return make_response(water_one_flow.get_sites(), 200, {})
    elif product == 'GetSiteInfo':
        ...
    elif product == 'GetVariables':
        ...
    elif product == 'GetVariableInfo':
        ...
    elif product == 'GetValues':
        return water_one_flow.get_values(request.args.get('location'), request.args.get('variable'),
                                         request.args.get('startDate'), request.args.get('endDate'))
    return jsonify({"status": "not-implemented"})


# >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> ERROR HANDLERS
@app.errorhandler(404)
def errors_404(e):
    if request.path.startswith(f'{api_path}'):
        return jsonify({"error": f'API Endpoint not found: {request.path} -> Check spelling and the API docs'}), 404
    return redirect(url_for('home')), 404, {'Refresh': f'1; url={url_for("home")}'}


@app.errorhandler(ValueError)
def error_valueerror(e):
    return jsonify({"error": str(e)}), 422


@app.errorhandler(AssertionError)
def error_assertion(e):
    return jsonify({"error": "invalid input argument", "code": 100})


@app.errorhandler(Exception)
def error_generalexception(e):
    return jsonify({"error": f"An unexpected error occurred: {e}", "code": 9999}), 500


if __name__ == '__main__':
    app.run()
