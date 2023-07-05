import os
import requests
import boto3
import logging
import time

GA_ID = os.getenv('GOOGLE_ANALYTICS_ID')
GA_TOKEN = os.getenv('GOOGLE_ANALYTICS_TOKEN')
LOG_GROUP_NAME = os.getenv('AWS_LOG_GROUP_NAME')
LOG_STREAM_NAME = os.getenv('AWS_LOG_STREAM_NAME')
# Create a CloudWatch Logs client
client = boto3.client('logs')
# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

products_v1 = (
    'ForecastStats', 'ForecastEnsembles', 'ForecastWarnings', 'ForecastRecords', 'HistoricSimulation', 'ReturnPeriods',
    'DailyAverages', 'MonthlyAverages', 'AvailableData', 'AvailableRegions', 'AvailableDates', 'GetReachID'
)

products_v2 = (
    'forecast', 'forecaststats', 'forecastensembles', 'forecastwarnings', 'forecastrecords',
    'forecastanomalies', 'historical', 'hindcast', 'returnperiods', 'dailyaverages', 'monthlyaverages',
    'availabledata', 'availableregions', 'availabledates', 'getreachid'
)

product_map_v1 = {}
for i, product in enumerate(products_v1):
    product_map_v1[product] = f'{(i + 1):02}'

product_map_v2 = {}
for i, product in enumerate(products_v2):
    product_map_v2[product] = f'{(i + 1):02}'


def track_event(version: str, product: str, reach_id: int) -> None:
    """
    Posts a custom event to the Google Analytics V4 reporting rest endpoint

    Requires environment variables
    - GOOGLE_ANALYTICS_ID: a Google Analytics property ID which is set up to receive events
    - GOOGLE_ANALYTICS_TOKEN: an auth token generated for the analytics property

    Example usage:
        track_event(
            product="Forecast",
            version="v2",
            reach_id=13001234
        )
    """

    event_name = f'{version}_{product.lower()}_{reach_id if reach_id is not None else 0}'
    data = {
        'client_id': 'geoglows',
        'events': [{
            'name': event_name,
            'params': {
                'value': 1
            }
        }],
    }
    requests.post(
        f'https://www.google-analytics.com/mp/collect?measurement_id={GA_ID}&api_secret={GA_TOKEN}',
        json=data
    )

def log_request(version: str, product: str, reach_id: int = None, **kwargs):
    """
    Posts a custom log to the aws cloudwatch logging service

    Requires environment variables
    - AWS_LOG_GROUP_NAME: the group name for the AWS CloudWatch log group within "logs".
    - AWS_LOG_STREAM_NAME: the stream name within the CloudWatch log group.

    Args:
        version: Either v1 or v2. Will be converted to a two digit code and placed at the beginning of the log message
        product: One of the product strings from the products tuple. Will be converted to a code using the product map
        reach_id: The unique id of the river being requested for. If not given, will be set to 00

        **kwargs:
            - region_no: Two digit code for the nga region (ex. 70 for north america)
            - source: Code defining source of request, i.e. app, api, etc. Defaults to 2

    Example usage:
        analytics.log_request(
            version="v2",
            product="forecaststats",
            reach_id=102001
        )

    Returns:
        the response from the CloudWatch service
    """

    # Construct the log message
    if reach_id is None:
        reach_id = 00
    product_code = product_map_v2[product] if version == 'v2' else product_map_v1[product]
    message = f'{version.replace("v", "0")}_{product_code}_{reach_id}'
    if 'region_no' in kwargs:
        message += f'_{kwargs["region_no"]}'
    if 'source' in kwargs:
        message += f'_{kwargs["source"]}'
    else:
        message += '_2'

    # Send the log message to CloudWatch
    response = client.put_log_events(
        logGroupName=LOG_GROUP_NAME,
        logStreamName=LOG_STREAM_NAME,
        logEvents=[
            {
                'timestamp': int(round(time.time() * 1000)),
                'message': message
            }
        ]
    )
    return response
