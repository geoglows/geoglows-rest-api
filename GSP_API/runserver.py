import logging
import sys
from os import getenv

from api_controller import (forecast_stats_handler, forecast_ensembles_handler, historic_data_handler,
                            return_periods_handler, seasonal_average_handler, get_region_handler,
                            get_available_dates_handler)
from flask import Flask, request, jsonify
from flask_restful import Api

from flask_cors import CORS, cross_origin

blob_mapped_dir = "/mnt/output"

print("Creating Application")

api_prefix = getenv('API_PREFIX')
app = Flask(__name__)

cors = CORS(app)
app.config['CORS_HEADERS'] = '*'

api = Api(app)
print(api_prefix)


# create logger function
def init_logger():
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    handler = logging.FileHandler('/app/api.log', 'a')
    formatter = logging.Formatter('%(asctime)s: %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)


# GET, API ForecastStats endpoint
@app.route(api_prefix + '/ForecastStats/', methods=['GET'])
@cross_origin()
def forecast_stats():
    init_logger()

    if request.args.get('reach_id', '') == '':
        if request.args.get('lat', '') == '' or request.args.get('lon', '') == '' or \
                request.args.get('region', '') == '':
            logging.error("Either reach_id or lat and lon parameters are required as input.")
            return jsonify({"error": "Either reach_id or lat and lon parameters are required as input."}), 422

    try:
        # Call the service
        return forecast_stats_handler(request)

    except:
        print(sys.exc_info()[0])
        logging.exception(sys.exc_info()[0])
        return jsonify({"error": "An unexpected error occurred."}), 400


# GET, API ForecastEnsembles endpoint
@app.route(api_prefix + '/ForecastEnsembles/', methods=['GET'])
@cross_origin()
def forecast_ensembles():
    init_logger()

    if request.args.get('reach_id', '') == '':
        if request.args.get('lat', '') == '' or request.args.get('lon', '') == '' or \
                request.args.get('region', '') == '':
            logging.error("Either reach_id or lat and lon parameters are required as input.")
            return jsonify({"error": "Either reach_id or lat and lon parameters are required as input."}), 422

    try:
        # Call the service
        return forecast_ensembles_handler(request)

    except:
        print(sys.exc_info()[0])
        logging.exception(sys.exc_info()[0])
        return jsonify({"error": "An unexpected error occured."}), 400


# GET, API HistoricSimulation endpoint
@app.route(api_prefix + '/HistoricSimulation/', methods=['GET'])
@cross_origin()
def historic_simulation():
    init_logger()

    if request.args.get('reach_id', '') == '':
        if request.args.get('lat', '') == '' or request.args.get('lon', '') == '' or \
                request.args.get('region', '') == '':
            logging.error("Either reach_id or lat and lon parameters are required as input.")
            return jsonify({"error": "Either reach_id or lat and lon parameters are required as input."}), 422

    try:
        # Call the service
        return historic_data_handler(request)

    except:
        print(sys.exc_info()[0])
        logging.exception(sys.exc_info()[0])
        return jsonify({"error": "An unexpected error occured."}), 400


# GET, API HistoricSimulation endpoint
@app.route(api_prefix + '/ReturnPeriods/', methods=['GET'])
@cross_origin()
def return_periods():
    init_logger()

    if request.args.get('reach_id', '') == '':
        if request.args.get('lat', '') == '' or request.args.get('lon', '') == '' or \
                request.args.get('region', '') == '':
            logging.error("Either reach_id or lat and lon parameters are required as input.")
            return jsonify({"error": "Either reach_id or lat and lon parameters are required as input."}), 422

    try:
        # Call the service
        return return_periods_handler(request)

    except:
        print(sys.exc_info()[0])
        logging.exception(sys.exc_info()[0])
        return jsonify({"error": "An unexpected error occured."}), 400


# GET, API HistoricSimulation endpoint
@app.route(api_prefix + '/SeasonalAverage/', methods=['GET'])
@cross_origin()
def seasonal_average():
    init_logger()

    if request.args.get('reach_id', '') == '':
        if request.args.get('lat', '') == '' or request.args.get('lon', '') == '' or \
                request.args.get('region', '') == '':
            logging.error("Either reach_id or lat and lon parameters are required as input.")
            return jsonify({"error": "Either reach_id or lat and lon parameters are required as input."}), 422

    try:
        # Call the service
        return seasonal_average_handler(request)

    except:
        print(sys.exc_info()[0])
        logging.exception(sys.exc_info()[0])
        return jsonify({"error": "An unexpected error occured."}), 400


@app.route(api_prefix + '/AvailableRegions/', methods=['GET'])
@cross_origin()
def regions():
    try:
        # Call the service
        return get_region_handler()

    except:
        print(sys.exc_info()[0])
        logging.exception(sys.exc_info()[0])
        return jsonify({"error": "An unexpected error occured."}), 400


@app.route(api_prefix + '/AvailableDates/', methods=['GET'])
@cross_origin()
def dates():
    if request.args.get('region', '') == '':
        logging.error("region is required as input.")
        return jsonify({"error": "region is required as input."}), 422

    try:
        # Call the service
        return get_available_dates_handler(request)

    except:
        print(sys.exc_info()[0])
        logging.exception(sys.exc_info()[0])
        return jsonify({"error": "An unexpected error occured."}), 400


if __name__ == '__main__':
    app.debug = True
    app.run()
