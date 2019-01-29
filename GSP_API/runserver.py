# /ai4e_api_tools has been added to the PYTHONPATH, so we can reference those
# libraries directly.
from flask import Flask, request#, jsonify
from flask_restful import Api
import json
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


# POST, API endpoint
@app.route(api_prefix + '/', methods=['POST'])
def post():
    try:
        post_body = json.loads(request.data)
        
    except:
        return "Unable to parse the request body. Please request with valid json."

    return get_streamflow(json_body = post_body)

def get_streamflow(**kwargs):
    init_logger()
    json_body = kwargs.get('json_body', None)
    
    if (not json_body):
        logging.error("Body is missing")
        return -1

    if (not "region" in json_body):
        logging.error("region is required as input.")
        return -1

    if (not "reach_id" in json_body):
        logging.error("reach_id is required as input.")
        return -1

    try:
        request = {"region": json_body["region"], "reach_id": json_body["reach_id"],
                   "stat": json_body["stat"], "return_format": json_body["return_format"]}

        # Call the service endpoint
        results = api_controller.get_ecmwf_forecast(request)

        return results

    except:
        print(sys.exc_info()[0])
        logging.exception(sys.exc_info()[0])
        return "An unexpected error occured"

if __name__ == '__main__':
    app.debug = True
    app.run()
