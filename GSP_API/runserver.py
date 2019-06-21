from flask import Flask, request, jsonify
from flask_restful import Api
import logging
import sys
from os import getenv

import api_controller

blob_mapped_dir = "/mnt/output"

print("Creating Application")

api_prefix = getenv('API_PREFIX')
app = Flask(__name__)

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
def forecast_stats():
    init_logger()

    if (request.args.get('region', '') == ''):
        logging.error("region is required as input.")
        return jsonify({"error": "region is required as input."}), 422

    if (request.args.get('reach_id', '') == ''):
        if request.args.get('lat', '') == '' or request.args.get('lon', '') == '':
            logging.error("Either reach_id or lat and lon parameters are required as input.")
            return jsonify({"error": "Either reach_id or lat and lon parameters are required as input."}), 422

    try:
        # Call the service
        results = api_controller.forecast_stats_handler(request)

        return results

    except:
        print(sys.exc_info()[0])
        logging.exception(sys.exc_info()[0])
        return jsonify({"error": "An unexpected error occured."}), 400


# GET, API ForecastEnsembles endpoint
@app.route(api_prefix + '/ForecastEnsembles/', methods=['GET'])
def forecast_ensembles():
    init_logger()

    if (request.args.get('region', '') == ''):
        logging.error("region is required as input.")
        return jsonify({"error": "region is required as input."}), 422

    if (request.args.get('reach_id', '') == ''):
        if request.args.get('lat', '') == '' or request.args.get('lon', '') == '':
            logging.error("Either reach_id or lat and lon parameters are required as input.")
            return jsonify({"error": "Either reach_id or lat and lon parameters are required as input."}), 422

    try:
        # Call the service
        results = api_controller.forecast_ensembles_handler(request)

        return results

    except:
        print(sys.exc_info()[0])
        logging.exception(sys.exc_info()[0])
        return jsonify({"error": "An unexpected error occured."}), 400
    

# GET, API HistoricSimulation endpoint
@app.route(api_prefix + '/HistoricSimulation/', methods=['GET'])
def historic_simulation():
    init_logger()

    if (request.args.get('region', '') == ''):
        logging.error("region is required as input.")
        return jsonify({"error": "region is required as input."}), 422

    if (request.args.get('reach_id', '') == ''):
        if request.args.get('lat', '') == '' or request.args.get('lon', '') == '':
            logging.error("Either reach_id or lat and lon parameters are required as input.")
            return jsonify({"error": "Either reach_id or lat and lon parameters are required as input."}), 422

    try:
        # Call the service
        results = api_controller.historic_data_handler(request)

        return results

    except:
        print(sys.exc_info()[0])
        logging.exception(sys.exc_info()[0])
        return jsonify({"error": "An unexpected error occured."}), 400


# GET, API HistoricSimulation endpoint
@app.route(api_prefix + '/ReturnPeriods/', methods=['GET'])
def return_periods():
    init_logger()

    if (request.args.get('region', '') == ''):
        logging.error("region is required as input.")
        return jsonify({"error": "region is required as input."}), 422

    if (request.args.get('reach_id', '') == ''):
        if request.args.get('lat', '') == '' or request.args.get('lon', '') == '':
            logging.error("Either reach_id or lat and lon parameters are required as input.")
            return jsonify({"error": "Either reach_id or lat and lon parameters are required as input."}), 422

    try:
        # Call the service
        results = api_controller.return_periods_handler(request)

        return results

    except:
        print(sys.exc_info()[0])
        logging.exception(sys.exc_info()[0])
        return jsonify({"error": "An unexpected error occured."}), 400
    

# GET, API HistoricSimulation endpoint
@app.route(api_prefix + '/SeasonalAverage/', methods=['GET'])
def seasonal_average():
    init_logger()

    if (request.args.get('region', '') == ''):
        logging.error("region is required as input.")
        return jsonify({"error": "region is required as input."}), 422

    if (request.args.get('reach_id', '') == ''):
        if request.args.get('lat', '') == '' or request.args.get('lon', '') == '':
            logging.error("Either reach_id or lat and lon parameters are required as input.")
            return jsonify({"error": "Either reach_id or lat and lon parameters are required as input."}), 422

    try:
        # Call the service
        results = api_controller.seasonal_average_handler(request)

        return results

    except:
        print(sys.exc_info()[0])
        logging.exception(sys.exc_info()[0])
        return jsonify({"error": "An unexpected error occured."}), 400
    
    
@app.route(api_prefix + '/AvailableRegions/', methods=['GET'])
def regions():
    
    try:
        # Call the service
        results = api_controller.get_region_handler()

        return results

    except:
        print(sys.exc_info()[0])
        logging.exception(sys.exc_info()[0])
        return jsonify({"error": "An unexpected error occured."}), 400


@app.route(api_prefix + '/AvailableDates/', methods=['GET'])
def dates():
    
    if (request.args.get('region', '') == ''):
        logging.error("region is required as input.")
        return jsonify({"error": "region is required as input."}), 422
    
    try:
        # Call the service
        results = api_controller.get_available_dates_handler(request)

        return results

    except:
        print(sys.exc_info()[0])
        logging.exception(sys.exc_info()[0])
        return jsonify({"error": "An unexpected error occured."}), 400


if __name__ == '__main__':
    app.debug = True
    app.run()
