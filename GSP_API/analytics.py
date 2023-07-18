import os
import boto3
import logging
import time

LOG_GROUP_NAME = os.getenv('AWS_LOG_GROUP_NAME')
LOG_STREAM_NAME = os.getenv('AWS_LOG_STREAM_NAME')
ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
REGION = os.getenv('AWS_REGION')

# Create a CloudWatch Logs client
client = boto3.client(
    'logs',
    aws_access_key_id=ACCESS_KEY_ID,
    aws_secret_access_key=SECRET_ACCESS_KEY,
    region_name=REGION
)

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

product_map_v1 = {
    'ForecastStats': '100',
    'ForecastEnsembles': '110',
    'ForecastWarnings': '120',
    'ForecastRecords': '130',
    'HistoricSimulation': '200',
    'ReturnPeriods': '210',
    'DailyAverages': '220',
    'MonthlyAverages': '230',
    'AvailableData': '300',
    'AvailableRegions': '310',
    'AvailableDates': '320',
    'GetReachID': '400'
}

product_map_v2 = {
    'forecaststats': '100',
    'forecastensembles': '110',
    'forecastwarnings': '120',
    'forecastrecords': '130',
    'forecast': '140',
    'forecastanomalies': '150',
    'hindcast': '200',
    'returnperiods': '210',
    'dailyaverages': '220',
    'monthlyaverages': '230',
    'historical': '240',
    'availabledata': '300',
    'availableregions': '310',
    'availabledates': '320',
    'getreachid': '400'
}

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
    if 'region_no' in kwargs:  #TODO: validate
        message += f'_{kwargs["region_no"]}'
    if kwargs.get('source') and kwargs.get('source').isdigit() and len(kwargs.get('source')) == 1:
        message += f'_{kwargs["source"]}'
    else:
        message += '_1'

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
