import datetime
import json

import pandas as pd
import s3fs
import xarray as xr
from flask import jsonify

from .constants import ODP_RETROSPECTIVE_S3_BUCKET_URI, ODP_S3_BUCKET_REGION, NUM_DECIMALS
from .data import get_return_periods_dataframe
from .response_formatters import df_to_csv_flask_response, df_to_jsonify_response

__all__ = ['retrospective', 'daily_averages', 'monthly_averages', 'yearly_averages', 'return_periods', ]


def _get_retrospective_df(reach_id: int) -> pd.DataFrame:
    # todo use pygeoglows
    s3 = s3fs.S3FileSystem(anon=True, client_kwargs=dict(region_name=ODP_S3_BUCKET_REGION))
    s3store = s3fs.S3Map(root=f'{ODP_RETROSPECTIVE_S3_BUCKET_URI}/retrospective.zarr', s3=s3, check=False)
    return (
        xr
        .open_zarr(s3store)
        .sel(rivid=reach_id)
        .to_dataframe()
        .reset_index()
        .set_index('time')
        .pivot(columns='rivid', values='Qout')
        .astype(float)
        .round(NUM_DECIMALS)
        .rename(columns=lambda x: str(x))
    )


def retrospective(reach_id: int, return_format: str, start_date: str = None,
                  end_date: str = None) -> pd.DataFrame:
    """ 
    Controller for retrieving simulated historic data
    """
    # todo use pygeoglows
    df = _get_retrospective_df(reach_id)

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
    # todo use pygeoglows
    df = _get_retrospective_df(reach_id)
    df = df.groupby(df.index.strftime('%m-%d')).mean()
    df.index = pd.to_datetime(df.index, format='%m-%d')

    if return_format == 'csv':
        return df_to_csv_flask_response(df, f'daily_averages_{reach_id}')
    if return_format == 'json':
        return df_to_jsonify_response(df=df, reach_id=reach_id)
    return df


def monthly_averages(reach_id: int, return_format: str):
    # todo use pygeoglows
    df = _get_retrospective_df(reach_id)
    df = df.groupby(df.index.strftime('%m')).mean()

    if return_format == 'csv':
        return df_to_csv_flask_response(df, f'monthly_averages_{reach_id}')
    if return_format == 'json':
        return df_to_jsonify_response(df=df, reach_id=reach_id)
    return df


def yearly_averages(reach_id, return_format):
    # todo use pygeoglows
    df = _get_retrospective_df(reach_id)
    df = df.groupby(df.index.strftime('%Y')).mean()

    if return_format == 'csv':
        return df_to_csv_flask_response(df, f'yearly_averages_{reach_id}')
    if return_format == 'json':
        return df_to_jsonify_response(df=df, reach_id=reach_id)
    return df


def return_periods(reach_id: int, return_format: str):
    df = get_return_periods_dataframe(reach_id)

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
