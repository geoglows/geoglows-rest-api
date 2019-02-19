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
        return jsonify({"error": "region is required as input."})

    if (request.args.get('reach_id', '') == ''):
        if request.args.get('lat', '') == '' or request.args.get('lon', '') == '':
            logging.error("Either reach_id or lat and lon parameters are required as input.")
            return jsonify({"error": "Either reach_id or lat and lon parameters are required as input."})

    try:
        # Call the service
        results = api_controller.forecast_stats_handler(request)

        return results

    except:
        print(sys.exc_info()[0])
        logging.exception(sys.exc_info()[0])
        return jsonify({"error": "An unexpected error occured."})


# GET, API ForecastEnsembles endpoint
@app.route(api_prefix + '/ForecastEnsembles/', methods=['GET'])
def forecast_ensembles():
    init_logger()

    if (request.args.get('region', '') == ''):
        logging.error("region is required as input.")
        return jsonify({"error": "region is required as input."})

    if (request.args.get('reach_id', '') == ''):
        if request.args.get('lat', '') == '' or request.args.get('lon', '') == '':
            logging.error("Either reach_id or lat and lon parameters are required as input.")
            return jsonify({"error": "Either reach_id or lat and lon parameters are required as input."})

    try:
        # Call the service
        results = api_controller.forecast_ensembles_handler(request)

        return results

    except:
        print(sys.exc_info()[0])
        logging.exception(sys.exc_info()[0])
        return jsonify({"error": "An unexpected error occured."})


if __name__ == '__main__':
    app.debug = True
    app.run()
