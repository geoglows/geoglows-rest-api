import logging
from os import getenv

from deprecated import (seasonal_average_handler,
                        deprecated_forecast_stats_handler,
                        deprecated_historic_data_handler)
from flask import Flask, render_template, request, jsonify, url_for, redirect
from flask_cors import CORS, cross_origin
from flask_restful import Api
from handlers_forecasts import (forecast_stats_handler,
                                forecast_ensembles_handler,
                                forecast_warnings_handler,
                                forecast_records_handler,
                                available_dates_handler, )
from handlers_historical import (historic_data_handler,
                                 historic_averages_handler,
                                 return_periods_handler, )
from handlers_utilities import (get_available_data_handler,
                                get_region_handler,
                                get_reach_id_from_latlon_handler, )

print("Creating Application")

api_path = getenv('API_PREFIX')
wof_path = 'wof'

app = Flask(__name__)
cors = CORS(app)
app.config['CORS_HEADERS'] = '*'
api = Api(app)


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


@app.route('/training')
@cross_origin()
def training():
    return render_template('training.html')


# >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> REST API ENDPOINTS
@app.route(f'{api_path}/ForecastStats/', methods=['GET'])
@cross_origin()
def forecast_stats():
    return forecast_stats_handler(request)


@app.route(f'{api_path}/ForecastEnsembles/', methods=['GET'])
@cross_origin()
def forecast_ensembles():
    return forecast_ensembles_handler(request)


@app.route(f'{api_path}/ForecastWarnings/', methods=['GET'])
@cross_origin()
def forecast_warnings():
    return forecast_warnings_handler(request)


@app.route(f'{api_path}/ForecastRecords/', methods=['GET'])
@cross_origin()
def forecast_records():
    return forecast_records_handler(request)


@app.route(f'{api_path}/HistoricSimulation/', methods=['GET'])
@cross_origin()
def historic_simulation():
    return historic_data_handler(request)


@app.route(f'{api_path}/ReturnPeriods/', methods=['GET'])
@cross_origin()
def return_periods():
    return return_periods_handler(request)


@app.route(f'{api_path}/DailyAverages/', methods=['GET'])
@cross_origin()
def daily_averages():
    return historic_averages_handler(request, 'daily')


@app.route(f'{api_path}/MonthlyAverages/', methods=['GET'])
@cross_origin()
def monthly_averages():
    return historic_averages_handler(request, 'monthly')


@app.route(f'{api_path}/AvailableData/', methods=['GET'])
@cross_origin()
def available_data():
    return get_available_data_handler()


@app.route(f'{api_path}/AvailableRegions/', methods=['GET'])
@cross_origin()
def regions():
    return get_region_handler()


@app.route(f'{api_path}/AvailableDates/', methods=['GET'])
@cross_origin()
def dates():
    if request.args.get('region', '') == '':
        raise ValueError('region is a required parameter')
    return available_dates_handler(request)


@app.route(f'{api_path}/GetReachID/', methods=['GET'])
@cross_origin()
def determine_reach_id():
    if request.args.get('lat', '') == '' or request.args.get('lon', '') == '':
        raise ValueError('Specify both a latitude (lat) and a longitude (lon)')
    return get_reach_id_from_latlon_handler(request)


# >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> ERROR HANDLERS
@app.errorhandler(404)
def errors_404(e):
    return redirect(url_for('home')), 404, {'Refresh': f'1; url={url_for("home")}'}


@app.errorhandler(ValueError)
def error_valueerror(e):
    return jsonify({"error": str(e)}), 422


@app.errorhandler(Exception)
def error_generalexception(e):
    return jsonify({"error": f"An unexpected error occurred: {e}"}), 500


# >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> DEPRECATED
# GET, API SeasonalAverage endpoint
@app.route(f'{api_path}/SeasonalAverage/', methods=['GET'])
@cross_origin()
def seasonal_average():
    # ensure you have enough information provided to return a response
    if request.args.get('reach_id', '') == '':
        if request.args.get('lat', '') == '' or request.args.get('lon', '') == '':
            logging.error("Either reach_id or lat and lon parameters are required as input.")
            return jsonify({"error": "Either reach_id or lat and lon parameters are required as input."}), 422

    return seasonal_average_handler(request)


@app.route(f'{api_path}/DeprecatedForecastStats/', methods=['GET'])
@cross_origin()
def deprecated_forecast_stats():
    # ensure you have enough information provided to return a response
    if request.args.get('reach_id', '') == '':
        if request.args.get('lat', '') == '' or request.args.get('lon', '') == '':
            logging.error("Either reach_id or lat and lon parameters are required as input.")
            return jsonify({"error": "Either reach_id or lat and lon parameters are required as input."}), 422

    return deprecated_forecast_stats_handler(request)


@app.route(f'{api_path}/DeprecatedHistoricSimulation/', methods=['GET'])
@cross_origin()
def deprecated_historic_simulation():
    # ensure you have enough information provided to return a response
    if request.args.get('reach_id', '') == '':
        if request.args.get('lat', '') == '' or request.args.get('lon', '') == '':
            logging.error("Either reach_id or lat and lon parameters are required as input.")
            return jsonify({"error": "Either reach_id or lat and lon parameters are required as input."}), 422

    return deprecated_historic_data_handler(request)


if __name__ == '__main__':
    app.debug = False
    app.run()
