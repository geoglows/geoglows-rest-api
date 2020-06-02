import logging
import sys
from os import getenv

from deprecated import (seasonal_average_handler)
from flask import Flask, request, jsonify
from flask import Flask, render_template, request, jsonify
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

api_prefix = getenv('API_PREFIX')
app = Flask(__name__)

cors = CORS(app)
app.config['CORS_HEADERS'] = '*'

api = Api(app)
print(api_prefix)

# GLOBAL
PATH_TO_ERA_INTERIM = '/mnt/output/era-interim'
PATH_TO_ERA_5 = '/mnt/output/era-5'
PATH_TO_FORECASTS = '/mnt/output/forecasts'
PATH_TO_FORECAST_RECORDS = '/mnt/output/forecast-records'


# create logger function
def init_logger():
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    handler = logging.FileHandler('/app/api.log', 'a')
    formatter = logging.Formatter('%(asctime)s: %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

# API home page
@app.route('/')
@cross_origin()
def home():
    return render_template('home.html')


# API documentation
@app.route('/documentation')
@cross_origin()
def documentation():
    return render_template('documentation.html')


# GET, API ForecastStats endpoint
@app.route(api_prefix + '/ForecastStats/', methods=['GET'])
@cross_origin()
def forecast_stats():
    init_logger()
    try:
        return forecast_stats_handler(request)
    except ValueError as e:
        return jsonify({"error": str(e)}), 422
    except Exception as e:
        print(sys.exc_info()[0])
        logging.exception(sys.exc_info()[0])
        return jsonify({"error": f"An unexpected error occurred: {e}"}), 400


# GET, API ForecastEnsembles endpoint
@app.route(api_prefix + '/ForecastEnsembles/', methods=['GET'])
@cross_origin()
def forecast_ensembles():
    init_logger()
    try:
        return forecast_ensembles_handler(request)
    except ValueError as e:
        return jsonify({"error": str(e)}), 422
    except Exception as e:
        print(sys.exc_info()[0])
        logging.exception(sys.exc_info()[0])
        return jsonify({"error": f"An unexpected error occurred: {e}"}), 400


# GET, API ForecastWarnings endpoint
@app.route(api_prefix + '/ForecastWarnings/', methods=['GET'])
@cross_origin()
def forecast_warnings():
    init_logger()
    try:
        return forecast_warnings_handler(request)
    except ValueError as e:
        return jsonify({"error": str(e)}), 422
    except Exception as e:
        print(sys.exc_info()[0])
        logging.exception(sys.exc_info()[0])
        return jsonify({"error": f"An unexpected error occurred: {e}"}), 400


# GET, API ForecastRecords endpoint
@app.route(api_prefix + '/ForecastRecords/', methods=['GET'])
@cross_origin()
def forecast_records():
    init_logger()
    try:
        return forecast_records_handler(request)
    except ValueError as e:
        return jsonify({"error": str(e)}), 422
    except Exception as e:
        print(sys.exc_info()[0])
        logging.exception(sys.exc_info()[0])
        return jsonify({"error": f"An unexpected error occurred: {e}"}), 400


# GET, API HistoricSimulation endpoint
@app.route(api_prefix + '/HistoricSimulation/', methods=['GET'])
@cross_origin()
def historic_simulation():
    init_logger()
    try:
        return historic_data_handler(request)
    except ValueError as e:
        return jsonify({"error": str(e)}), 422
    except Exception as e:
        print(sys.exc_info()[0])
        logging.exception(sys.exc_info()[0])
        return jsonify({"error": f"An unexpected error occurred: {e}"}), 400


# GET, API ReturnPeriods endpoint
@app.route(api_prefix + '/ReturnPeriods/', methods=['GET'])
@cross_origin()
def return_periods():
    init_logger()
    try:
        return return_periods_handler(request)
    except ValueError as e:
        return jsonify({"error": str(e)}), 422
    except Exception as e:
        print(sys.exc_info()[0])
        logging.exception(sys.exc_info()[0])
        return jsonify({"error": f"An unexpected error occurred: {e}"}), 400


# GET, API SeasonalAverage endpoint
@app.route(api_prefix + '/DailyAverages/', methods=['GET'])
@cross_origin()
def daily_averages():
    init_logger()
    try:
        return historic_averages_handler(request, 'daily')
    except ValueError as e:
        return jsonify({"error": str(e)}), 422
    except Exception as e:
        print(sys.exc_info()[0])
        logging.exception(sys.exc_info()[0])
        return jsonify({"error": f"An unexpected error occurred: {e}"}), 400


# GET, API SeasonalAverage endpoint
@app.route(api_prefix + '/MonthlyAverages/', methods=['GET'])
@cross_origin()
def monthly_averages():
    init_logger()
    try:
        return historic_averages_handler(request, 'monthly')
    except ValueError as e:
        return jsonify({"error": str(e)}), 422
    except Exception as e:
        print(sys.exc_info()[0])
        logging.exception(sys.exc_info()[0])
        return jsonify({"error": f"An unexpected error occurred: {e}"}), 400


# GET, API AvailableData endpoint
@app.route(api_prefix + '/AvailableData/', methods=['GET'])
@cross_origin()
def available_data():
    init_logger()

    try:
        return get_available_data_handler()
    except ValueError as e:
        return jsonify({"error": str(e)}), 422
    except Exception as e:
        print(sys.exc_info()[0])
        logging.exception(sys.exc_info()[0])
        return jsonify({"error": f"An unexpected error occurred: {e}"}), 400


# GET, API AvailableRegions endpoint
@app.route(api_prefix + '/AvailableRegions/', methods=['GET'])
@cross_origin()
def regions():
    init_logger()
    try:
        return get_region_handler()
    except ValueError as e:
        return jsonify({"error": str(e)}), 422
    except Exception as e:
        print(sys.exc_info()[0])
        logging.exception(sys.exc_info()[0])
        return jsonify({"error": f"An unexpected error occurred: {e}"}), 400


# GET, API AvailableDates endpoint
@app.route(api_prefix + '/AvailableDates/', methods=['GET'])
@cross_origin()
def dates():
    if request.args.get('region', '') == '':
        return jsonify({"error": "region is required as input."}), 422

    try:
        # Call the service
        return available_dates_handler(request)

    except ValueError as e:
        return jsonify({"error": str(e)}), 422
    except Exception as e:
        print(sys.exc_info()[0])
        logging.exception(sys.exc_info()[0])
        return jsonify({"error": f"An unexpected error occurred: {e}"}), 400


# GET, API GetReachID endpoint
@app.route(api_prefix + '/GetReachID/', methods=['GET'])
@cross_origin()
def determine_reach_id():
    """
    returns the reach_id closest to a given latitude/longitude pair
    """
    if request.args.get('lat', '') == '' or request.args.get('lon', '') == '':
        logging.error("Specify both a latitude (lat) and a longitude (lon).")
        return jsonify({"error": "Specify both a latitude (lat) and a longitude (lon)."}), 422

    try:
        return get_reach_id_from_latlon_handler(request)
    except ValueError as e:
        return jsonify({"error": str(e)}), 422
    except Exception as e:
        print(sys.exc_info()[0])
        logging.exception(sys.exc_info()[0])
        return jsonify({"error": f"An unexpected error occurred: {e}"}), 400


# >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> DEPRECATED
# GET, API SeasonalAverage endpoint
@app.route(api_prefix + '/SeasonalAverage/', methods=['GET'])
@cross_origin()
def seasonal_average():
    init_logger()

    # ensure you have enough information provided to return a response
    if request.args.get('reach_id', '') == '':
        if request.args.get('lat', '') == '' or request.args.get('lon', '') == '':
            logging.error("Either reach_id or lat and lon parameters are required as input.")
            return jsonify({"error": "Either reach_id or lat and lon parameters are required as input."}), 422

    try:
        return seasonal_average_handler(request)
    except ValueError as e:
        return jsonify({"error": str(e)}), 422
    except Exception as e:
        print(sys.exc_info()[0])
        logging.exception(sys.exc_info()[0])
        return jsonify({"error": f"An unexpected error occurred: {e}"}), 400


if __name__ == '__main__':
    app.debug = True
    app.run()
