import os
import requests

GA_ID = os.getenv('GOOGLE_ANALYTICS_ID')
GA_TOKEN = os.getenv('GOOGLE_ANALYTICS_TOKEN')


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
