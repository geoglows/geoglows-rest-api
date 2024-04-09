import datetime
import json

import geoglows
import numpy as np
import pandas as pd
from flask import jsonify

from .constants import NUM_DECIMALS, PACKAGE_METADATA_TABLE_PATH
from .data import (get_forecast_dataset,
                   get_forecast_records_dataset,
                   find_available_dates, )
from .response_formatters import (df_to_jsonify_response,
                                  df_to_csv_flask_response,
                                  new_json_template, )

__all__ = ['hydroviewer', 'forecast', 'forecast_stats', 'forecast_ensembles', 'forecast_records', 'forecast_dates']

geoglows.METADATA_TABLE_PATH = PACKAGE_METADATA_TABLE_PATH


def hydroviewer(reach_id: int, start_date: str, date: str) -> jsonify:
    if date == 'latest':
        date = find_available_dates()[-1]
    forecast_df = forecast(reach_id, date, "df")
    records_df = forecast_records(reach_id, start_date=start_date, end_date=date[:8], return_format="df")
    return_periods = geoglows.data.return_periods(reach_id)

    # add the columns from the dataframe
    json_template = new_json_template(
        reach_id,
        start_date=records_df.index[0],
        end_date=forecast_df.index[-1]
    )
    json_template['metadata']['series'] = [
        'datetime_records',
        'datetime_forecast',
        'return_periods',
    ] + forecast_df.columns.tolist() + records_df.columns.tolist()
    json_template.update(forecast_df.to_dict(orient='list'))
    json_template.update(records_df.to_dict(orient='list'))
    json_template['return_periods'] = return_periods.to_dict(orient='records')[0]
    return jsonify(json_template), 200


def forecast(reach_id: int, date: str, return_format: str) -> pd.DataFrame:
    forecast_xarray_dataset = get_forecast_dataset(reach_id, date)

    # get an array of all the ensembles, delete the high res before doing averages
    merged_array = forecast_xarray_dataset.data
    merged_array = np.delete(merged_array, list(forecast_xarray_dataset.ensemble.data).index(52), axis=0)

    # replace any values that went negative because of the routing
    merged_array[merged_array <= 0] = 0

    # load all the series into a dataframe
    df = (
        pd.DataFrame({
            f'flow_uncertainty_upper': np.nanpercentile(merged_array, 80, axis=0),
            f'flow_median': np.median(merged_array, axis=0),
            f'flow_uncertainty_lower': np.nanpercentile(merged_array, 20, axis=0),
        }, index=forecast_xarray_dataset.time.data)
        .dropna()
        .astype(np.float64)
        .round(NUM_DECIMALS)
    )
    df.index = df.index.strftime('%Y-%m-%dT%X+00:00')
    df.index.name = 'datetime'

    if return_format == 'csv':
        return df_to_csv_flask_response(df, f'forecast_{reach_id}')
    if return_format == 'json':
        return df_to_jsonify_response(df=df, reach_id=reach_id)
    return df


def forecast_stats(reach_id: int, date: str, return_format: str) -> pd.DataFrame:
    forecast_xarray_dataset = get_forecast_dataset(reach_id, date)

    # get an array of all the ensembles, delete the high res before doing averages
    merged_array = forecast_xarray_dataset.data
    merged_array = np.delete(merged_array, forecast_xarray_dataset.ensemble.data.tolist().index(52), axis=0)

    # replace any values that went negative because of the routing
    merged_array[merged_array <= 0] = 0

    # load all the series into a dataframe
    df = pd.DataFrame({
        f'flow_max': np.amax(merged_array, axis=0),
        f'flow_75p': np.nanpercentile(merged_array, 75, axis=0),
        f'flow_avg': np.mean(merged_array, axis=0),
        f'flow_med': np.median(merged_array, axis=0),
        f'flow_25p': np.nanpercentile(merged_array, 25, axis=0),
        f'flow_min': np.min(merged_array, axis=0),
        f'high_res': forecast_xarray_dataset.sel(ensemble=52).data,
    }, index=forecast_xarray_dataset.time.data)
    df.index = df.index.strftime('%Y-%m-%dT%X+00:00')
    df.index.name = 'datetime'
    df = df.astype(np.float64).round(NUM_DECIMALS)

    if return_format == 'csv':
        return df_to_csv_flask_response(df, f'forecast_stats_{reach_id}')
    if return_format == 'json':
        return df_to_jsonify_response(df=df, reach_id=reach_id)
    if return_format == "df":
        return df


def forecast_ensembles(reach_id: int, date: str, return_format: str, ensemble: str):
    forecast_xarray_dataset = get_forecast_dataset(reach_id, date)

    # make a list column names (with zero padded numbers) for the pandas DataFrame
    ensemble_column_names = []
    for i in range(1, 53):
        ensemble_column_names.append(f'ensemble_{i:02}')

    # make the data into a pandas dataframe
    df = pd.DataFrame(data=np.transpose(forecast_xarray_dataset.data).round(NUM_DECIMALS),
                      columns=ensemble_column_names,
                      index=forecast_xarray_dataset.time.data)
    df.index = df.index.strftime('%Y-%m-%dT%X+00:00')
    df.index.name = 'datetime'
    df = df.astype(np.float64).round(NUM_DECIMALS)

    # filtering which ensembles you want to get out of the dataframe of them all
    if ensemble != 'all':
        requested_ensembles = []
        for ens in ensemble.split(','):
            # if there was a range requested with a '-', generate a list of numbers between the 2
            if '-' in ens:
                start, end = ens.split('-')
                for i in range(int(start), int(end) + 1):
                    requested_ensembles.append(f'ensemble_{int(i):02}')
            else:
                requested_ensembles.append(f'ensemble_{int(ens):02}')
        # make a list of columns to remove from the dataframe deleting the requested ens from all ens columns
        for ens in requested_ensembles:
            if ens in ensemble_column_names:
                ensemble_column_names.remove(ens)
        # delete the dataframe columns we aren't interested
        for ens in ensemble_column_names:
            del df[ens]

    if return_format == 'csv':
        return df_to_csv_flask_response(df, f'forecast_ensembles_{reach_id}')
    if return_format == 'json':
        return df_to_jsonify_response(df=df, reach_id=reach_id)
    if return_format == "df":
        return df


def forecast_records(reach_id: int, start_date: str, end_date: str, return_format: str) -> pd.DataFrame:
    if start_date is None:
        start_date = datetime.datetime.now() - datetime.timedelta(days=14)
        start_date = start_date.strftime('%Y%m%d')
    if end_date is None:
        end_date = f'{datetime.datetime.now().year + 1}0101'
    year = start_date[:4]

    try:
        start_date = pd.to_datetime(start_date)
        end_date = pd.to_datetime(end_date)
    except ValueError:
        ValueError(f'Unrecognized date format for the start_date or end_date. Use YYYYMMDD format.')

    vpu = geoglows.streams.reach_to_vpu(reach_id)
    ds = get_forecast_records_dataset(vpu=vpu, year=year)

    # create a dataframe and filter by date
    df = (
        ds
        .sel(rivid=reach_id)
        .Qout
        .to_dataframe()
        .loc[start_date:end_date]
        .dropna()
        .pivot(columns='rivid', values='Qout')
    )
    df.columns = ['average_flow', ]
    df['average_flow'] = df['average_flow'].astype(float).round(NUM_DECIMALS)
    df.index = df.index.strftime('%Y-%m-%dT%X+00:00')

    # create the http response
    if return_format == 'csv':
        return df_to_csv_flask_response(df, f'forecast_records_{reach_id}')
    if return_format == 'json':
        return df_to_jsonify_response(df=df, reach_id=reach_id)
    if return_format == "df":
        return df


def forecast_dates(return_format: str):
    dates = find_available_dates()
    if return_format == 'csv':
        return df_to_csv_flask_response(pd.DataFrame(dates, columns=['dates', ]), f'forecast_dates', index=False)
    elif return_format == 'json':
        return jsonify({'dates': dates})
    else:
        raise ValueError(f'Unsupported return format requested: {return_format}')
