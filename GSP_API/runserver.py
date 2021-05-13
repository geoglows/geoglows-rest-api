import logging
from os import getenv

from flask import Flask, render_template, request, jsonify, url_for, redirect
from flask_cors import CORS, cross_origin
from flask_restful import Api
from handlers_forecasts import (forecast_stats,
                                forecast_ensembles,
                                forecast_warnings,
                                forecast_records,
                                available_dates, )
from handlers_historical import (historical,
                                 historical_averages,
                                 return_periods, )
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
@app.route(f'{api_path}/<api_version>/<product>/<reachid>', methods=['GET'])
@cross_origin()
def rest_endpoints(api_version: str, product: str, reachid: int):
    api_version = str(api_version).lower()
    product = str(product).lower()
    reachid = int(reachid)

    forecast_methods = ('forecast', 'forecaststats', 'forecastensembles', 'forecastwarnings', 'forecastrecords')

    if product not in forecast_methods:
        return errors_404()

    # forecast data products
    if product == 'forecast':
        return jsonify({"status": "success"})
    elif product == 'forecaststats':
        return forecast_stats(request)
    elif product == 'forecastensembles':
        return forecast_ensembles(request)
    elif product == 'forecastwarnings':
        return forecast_warnings(request)
    elif product == 'forecastrecords':
        return forecast_records(request)

    # historical data products
    elif product in ('historical', 'historicalsimulation'):
        return historical(request)
    elif product == 'returnperiods':
        return return_periods(request)
    elif product == 'dailyaverages':
        return historical_averages(request, 'daily')
    elif product == 'monthlyaverages':
        return historical_averages(request, 'monthly')

    return jsonify({"status": "success"})


# >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> WATERONEFLOW ENDPOINTS
@app.route(f'{api_path}/{wof_path}/<product>', methods=['GET'])
@cross_origin()
def wof_endpoints(product: str):
    return jsonify({"status": "not-implemented"})


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
    return available_dates(request)


@app.route(f'{api_path}/GetReachID/', methods=['GET'])
@cross_origin()
def determine_reach_id():
    return get_reach_id_from_latlon_handler(request)


# >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> ERROR HANDLERS
@app.errorhandler(404)
def errors_404():
    if request.path.startswith(f'{api_path}'):
        return jsonify({"error": f'API Endpoint not found: {request.path} -> Check spelling and the API docs'}), 404
    return redirect(url_for('home')), 404, {'Refresh': f'1; url={url_for("home")}'}


@app.errorhandler(ValueError)
def error_valueerror(e):
    return jsonify({"error": str(e)}), 422


@app.errorhandler(Exception)
def error_generalexception(e):
    return jsonify({"error": f"An unexpected error occurred: {e}"}), 500


if __name__ == '__main__':
    app.debug = False
    app.run()
