import os
import requests

from v2_utilities import ALL_PRODUCTS as product_list

GA_ID = os.getenv('GOOGLE_ANALYTICS_ID')
GA_TOKEN = os.getenv('GOOGLE_ANALYTICS_TOKEN')


def track_event(version: str, product: str, reach_id: int, return_format: str) -> None:
    """
    Posts a custom event to the Google Analytics V4 reporting rest endpoint
    Refer to https://developers.google.com/analytics/devguides/collection/protocol/ga4/sending-events?client_type=gtag

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

    event_name = f'{version}_{product_list[product]}_{reach_id if reach_id is not None else 0}_{return_format}'
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
