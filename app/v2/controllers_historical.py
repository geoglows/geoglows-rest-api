import datetime
import json

import geoglows
import pandas as pd
from flask import jsonify

from .constants import PACKAGE_METADATA_TABLE_PATH
from .response_formatters import df_to_csv_flask_response, df_to_jsonify_response

__all__ = ['retrospective', 'daily_averages', 'monthly_averages', 'yearly_averages', 'return_periods', ]

geoglows.METADATA_TABLE_PATH = PACKAGE_METADATA_TABLE_PATH


def retrospective(reach_id: int, return_format: str, start_date: str = None,
                  end_date: str = None) -> pd.DataFrame:
    """ 
    Controller for retrieving simulated historic data
    """
    df = geoglows.data.retrospective(reach_id)
    df.columns = df.columns.astype(str)
    df = df.astype(float).round(2)

    if start_date is not None:
        df = df.loc[df.index >= datetime.datetime.strptime(start_date, '%Y%m%d')]
    if end_date is not None:
        df = df.loc[df.index <= datetime.datetime.strptime(end_date, '%Y%m%d')]

    if return_format == 'csv':
        return df_to_csv_flask_response(df, f'retrospective_{reach_id}')
    if return_format == 'json':
        return df_to_jsonify_response(df=df, reach_id=reach_id)
    return df


def daily_averages(reach_id: int, return_format: str):
    df = geoglows.data.daily_averages(reach_id)
    df.columns = df.columns.astype(str)

    if return_format == 'csv':
        return df_to_csv_flask_response(df, f'daily_averages_{reach_id}')
    if return_format == 'json':
        return df_to_jsonify_response(df=df, reach_id=reach_id)
    return df


def monthly_averages(reach_id: int, return_format: str):
    df = geoglows.data.monthly_averages(reach_id)
    df.columns = df.columns.astype(str)
    df = df.astype(float).round(2)

    if return_format == 'csv':
        return df_to_csv_flask_response(df, f'monthly_averages_{reach_id}')
    if return_format == 'json':
        return df_to_jsonify_response(df=df, reach_id=reach_id)
    return df


def yearly_averages(reach_id, return_format):
    df = geoglows.data.annual_averages(reach_id)
    df.columns = df.columns.astype(str)
    df = df.astype(float).round(2)

    if return_format == 'csv':
        return df_to_csv_flask_response(df, f'yearly_averages_{reach_id}')
    if return_format == 'json':
        return df_to_jsonify_response(df=df, reach_id=reach_id)
    return df


def return_periods(reach_id: int, return_format: str):
    df = geoglows.data.return_periods(reach_id)
    df.columns = df.columns.astype(str)
    df = df.astype(float).round(2)

    if return_format == 'csv':
        return df_to_csv_flask_response(df, f'return_periods_{reach_id}')
    if return_format == 'json':
        return jsonify({
            'return_periods': json.loads(df.to_json(orient='records'))[0],
            'reach_id': reach_id,
            'gen_date': datetime.datetime.now(datetime.UTC).strftime('%Y-%m-%dT%X+00:00'),
            'units': {
                'name': 'streamflow',
                'short': 'cms',
                'long': f'cubic meters per second',
            },
        })
