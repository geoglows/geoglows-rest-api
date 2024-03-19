import datetime
import json

import hydrostats.data as hd
import numpy as np
import pandas as pd
from flask import jsonify

from .constants import NUM_DECIMALS
from .v2_utilities import get_historical_dataframe, dataframe_to_jsonify_response, dataframe_to_csv_flask_response, \
    get_return_periods_dataframe

__all__ = ['historical', 'historical_averages', 'return_periods']


def historical(reach_id: int, units: str, return_format: str) -> pd.DataFrame:
    """
    Controller for retrieving simulated historic data
    """

    df = get_historical_dataframe(reach_id, units)
    df = df.astype(np.float64).round(NUM_DECIMALS)

    if return_format == 'csv':
        return dataframe_to_csv_flask_response(df, f'hindcast_{reach_id}_{units}')
    if return_format == 'json':
        return dataframe_to_jsonify_response(df=df, reach_id=reach_id, units=units)
    if return_format == 'df':
        return df


def historical_averages(reach_id, units, average_type, return_format):
    df = get_historical_dataframe(reach_id, units)

    df.index = pd.to_datetime(df.index)

    if average_type == 'daily':
        df = hd.daily_average(df, rolling=True)
    else:
        df = hd.monthly_average(df)
    df.index.name = 'datetime'
    df = df.astype(np.float64).round(NUM_DECIMALS)

    if return_format == 'csv':
        return dataframe_to_csv_flask_response(df, f'{average_type}_averages_{reach_id}_{units}')
    if return_format == 'json':
        return dataframe_to_jsonify_response(df=df, reach_id=reach_id, units=units)
    return df


def return_periods(reach_id: int, units: str, return_format: str):
    df = get_return_periods_dataframe(reach_id, units)
    df = df.astype(np.float64).round(NUM_DECIMALS)

    if return_format == 'csv':
        return dataframe_to_csv_flask_response(df, f'return_periods_{reach_id}_{units}')
    if return_format == 'json':
        return jsonify({
            'return_periods': json.loads(df.to_json(orient='records'))[0],
            'reach_id': reach_id,
            'simulation_forcing': 'ERA5',
            'gen_date': datetime.datetime.utcnow().strftime('%Y-%m-%dY%X+00:00'),
            'units': {
                'name': 'streamflow',
                'short': f'{units}',
                'long': f'Cubic {"Meters" if units == "cms" else "Feet"} per Second',
            },
        })
