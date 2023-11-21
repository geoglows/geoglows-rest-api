import datetime
import json

import geoglows
import pandas as pd
from flask import jsonify

from .constants import M3_TO_FT3
from .data import get_return_periods_dataframe
from .response_formatters import df_to_csv_flask_response, df_to_jsonify_response

__all__ = ['retrospective', 'daily_averages', 'monthly_averages', 'yearly_averages', 'return_periods', ]


def retrospective(reach_id: int, units: str, return_format: str, start_date: str = None,
                  end_date: str = None) -> pd.DataFrame:
    """
    Controller for retrieving simulated historic data
    """
    df = geoglows.data.retrospective(reach_id=reach_id)
    if units == 'cfs':
        df *= M3_TO_FT3
    if start_date is not None:
        df = df.loc[datetime.datetime.strptime(start_date, '%Y%m%d'):]
    if end_date is not None:
        df = df.loc[:datetime.datetime.strptime(end_date, '%Y%m%d')]

    if return_format == 'csv':
        return df_to_csv_flask_response(df, f'hindcast_{reach_id}_{units}')
    if return_format == 'json':
        return df_to_jsonify_response(df=df, reach_id=reach_id, units=units)
    return df


def daily_averages(reach_id: int, units: str, return_format: str):
    df = geoglows.data.daily_averages(reach_id=reach_id)

    if units == 'cfs':
        df *= M3_TO_FT3

    if return_format == 'csv':
        return df_to_csv_flask_response(df, f'daily_averages_{reach_id}_{units}')
    if return_format == 'json':
        return df_to_jsonify_response(df=df, reach_id=reach_id, units=units)
    return df


def monthly_averages(reach_id: int, units: str, return_format: str):
    df = geoglows.data.monthly_averages(reach_id=reach_id)

    if units == 'cfs':
        df *= M3_TO_FT3

    if return_format == 'csv':
        return df_to_csv_flask_response(df, f'monthly_averages_{reach_id}_{units}')
    if return_format == 'json':
        return df_to_jsonify_response(df=df, reach_id=reach_id, units=units)
    return df


def yearly_averages(reach_id, units, return_format):
    df = geoglows.data.yearly_averages(reach_id=reach_id)

    if units == 'cfs':
        df *= M3_TO_FT3

    if return_format == 'csv':
        return df_to_csv_flask_response(df, f'yearly_averages_{reach_id}_{units}')
    if return_format == 'json':
        return df_to_jsonify_response(df=df, reach_id=reach_id, units=units)
    return df


def return_periods(reach_id: int, units: str, return_format: str):
    df = get_return_periods_dataframe(reach_id, units)

    if return_format == 'csv':
        return df_to_csv_flask_response(df, f'return_periods_{reach_id}_{units}')
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
