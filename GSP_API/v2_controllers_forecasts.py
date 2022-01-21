import datetime
import os
from datetime import datetime as dt

import numpy as np
import pandas as pd
import xarray
from flask import jsonify

from constants import PATH_TO_FORECAST_RECORDS, M3_TO_FT3
from model_utilities import reach_to_region

import v2_utilities
from v2_controllers_historical import historical_averages

__all__ = ['forecast_stats', 'forecast_ensembles', 'forecast_records', 'forecast_anomalies']


def forecast(reach_id, date, units, return_format):
    forecast_xarray_dataset = v2_utilities.get_forecast_dataset(reach_id, date)

    # get an array of all the ensembles, delete the high res before doing averages
    merged_array = forecast_xarray_dataset.data
    merged_array = np.delete(merged_array, list(forecast_xarray_dataset.ensemble.data).index(52), axis=0)

    # replace any values that went negative because of the routing
    merged_array[merged_array <= 0] = 0

    # load all the series into a dataframe
    df = pd.DataFrame({
        f'flow_avg_{units}': np.mean(merged_array, axis=0),
    }, index=forecast_xarray_dataset.time.data)
    df.dropna(inplace=True)
    df.index = df.index.strftime('%Y-%m-%dT%X+00:00')
    df.index.name = 'datetime'

    # handle units conversion
    if units == 'cfs':
        for column in df.columns:
            df[column] *= M3_TO_FT3

    if return_format == 'csv':
        return v2_utilities.dataframe_to_csv_flask_response(df, f'streamflow_forecast_{reach_id}_{units}.csv')
    if return_format == 'json':
        return v2_utilities.dataframe_to_jsonify_response(df=df, df_highres=None, reach_id=reach_id, units=units)
    return df


def forecast_stats(reach_id, date, units, return_format):
    forecast_xarray_dataset = v2_utilities.get_forecast_dataset(reach_id, date)

    # get an array of all the ensembles, delete the high res before doing averages
    merged_array = forecast_xarray_dataset.data
    merged_array = np.delete(merged_array, list(forecast_xarray_dataset.ensemble.data).index(52), axis=0)

    # replace any values that went negative because of the routing
    merged_array[merged_array <= 0] = 0

    # load all the series into a dataframe
    df = pd.DataFrame({
        f'flow_max_{units}': np.amax(merged_array, axis=0),
        f'flow_75p_{units}': np.percentile(merged_array, 75, axis=0),
        f'flow_avg_{units}': np.mean(merged_array, axis=0),
        f'flow_med_{units}': np.median(merged_array, axis=0),
        f'flow_25p_{units}': np.percentile(merged_array, 25, axis=0),
        f'flow_min_{units}': np.min(merged_array, axis=0),
        f'high_res_{units}': forecast_xarray_dataset.sel(ensemble=52).data
    }, index=forecast_xarray_dataset.time.data)
    df.index = df.index.strftime('%Y-%m-%dT%X+00:00')
    df.index.name = 'datetime'

    # handle units conversion
    if units == 'cfs':
        for column in df.columns:
            df[column] *= M3_TO_FT3

    if return_format == 'csv':
        return v2_utilities.dataframe_to_csv_flask_response(df, f'streamflow_forecast_stats_{reach_id}_{units}.csv')
    if return_format == 'json':
        df_highres = df[f'high_res_{units}'].dropna()
        return v2_utilities.dataframe_to_jsonify_response(df=df, df_highres=df_highres, reach_id=reach_id, units=units)
    if return_format == "df":
        return df


def forecast_ensembles(reach_id, date, units, return_format, ensemble):
    forecast_xarray_dataset = v2_utilities.get_forecast_dataset(reach_id, date)

    # make a list column names (with zero padded numbers) for the pandas DataFrame
    ensemble_column_names = []
    for i in range(1, 53):
        ensemble_column_names.append(f'ensemble_{i:02}_{units}')

    # make the data into a pandas dataframe
    df = pd.DataFrame(data=np.transpose(forecast_xarray_dataset.data),
                      columns=ensemble_column_names,
                      index=forecast_xarray_dataset.time.data)
    df.index = df.index.strftime('%Y-%m-%dT%X+00:00')
    df.index.name = 'datetime'

    # handle units conversion
    if units == 'cfs':
        for column in df.columns:
            df[column] *= M3_TO_FT3

    # filtering which ensembles you want to get out of the dataframe of them all
    if ensemble != 'all':
        requested_ensembles = []
        for ens in ensemble.split(','):
            # if there was a range requested with a '-', generate a list of numbers between the 2
            if '-' in ens:
                start, end = ens.split('-')
                for i in range(int(start), int(end) + 1):
                    requested_ensembles.append(f'ensemble_{int(i):02}_{units}')
            else:
                requested_ensembles.append(f'ensemble_{int(ens):02}_{units}')
        # make a list of columns to remove from the dataframe deleting the requested ens from all ens columns
        for ens in requested_ensembles:
            if ens in ensemble_column_names:
                ensemble_column_names.remove(ens)
        # delete the dataframe columns we aren't interested
        for ens in ensemble_column_names:
            del df[ens]

    if return_format == 'csv':
        return v2_utilities.dataframe_to_csv_flask_response(df, f'streamflow_forecast_ensembles_{reach_id}_{units}.csv')
    if return_format == 'json':
        json_template = v2_utilities.new_json_template(reach_id, units, df.index[0], df.index[-1])
        json_template['time_series'] = {
            'datetime': df[f'ensemble_01_{units}'].dropna(inplace=False).index.tolist(),
            'datetime_high_res': df[f'ensemble_52_{units}'].dropna(inplace=False).index.tolist(),
        }
        for column in df.columns:
            json_template['time_series'][column] = df[column].dropna(inplace=False).tolist()
        return jsonify(json_template)
    if return_format == "df":
        return df


def forecast_records(reach_id, start_date, end_date, units, return_format):
    region = reach_to_region(reach_id)
    try:
        start_date = dt.strptime(start_date, '%Y%m%d')
        end_date = dt.strptime(end_date, '%Y%m%d')
    except:
        raise ValueError(f'Unrecognized start_date "{start_date}" or end_date "{end_date}". Use YYYYMMDD format')

    # open and read the forecast record netcdf
    record_path = os.path.join(PATH_TO_FORECAST_RECORDS, region,
                               f'forecast_record-{datetime.datetime.utcnow().year}-{region}.nc')
    forecast_record = xarray.open_dataset(record_path)
    times = pd.to_datetime(pd.Series(forecast_record['time'].data, name='datetime'), unit='s', origin='unix')
    record_flows = forecast_record.sel(rivid=reach_id)['Qout']
    forecast_record.close()

    # create a dataframe and filter by date
    df = times.to_frame().join(pd.Series(record_flows, name=f'streamflow_{units}'))
    df = df[df['datetime'].between(start_date, end_date)]
    df.index = df['datetime']
    del df['datetime']
    df.index = df.index.strftime('%Y-%m-%dT%H:%M:%SZ')
    df.index.name = 'datetime'
    df.dropna(inplace=True)

    if units == 'cfs':
        df[f'streamflow_{units}'] *= M3_TO_FT3

    # create the http response
    if return_format == 'csv':
        return v2_utilities.dataframe_to_csv_flask_response(df, f'forecast_record_{reach_id}.csv')
    if return_format == 'json':
        return v2_utilities.dataframe_to_jsonify_response(df=df, df_highres=None, reach_id=reach_id, units=units)
    if return_format == "df":
        return df


def forecast_anomalies(reach_id, date, units, return_format):
    df = forecast(reach_id, date, units, "df")
    df = df.dropna()
    avg_df = historical_averages(reach_id, units, 'daily', 'df')

    df['datetime'] = df.index
    df.index = pd.to_datetime(df.index).strftime("%m/%d")
    df = df.join(avg_df, how="inner")
    df[f'anomaly_{units}'] = df[f'flow_avg_{units}'] - df[f'flow_{units}']
    df.index = df['datetime']
    df = df.rename(columns={f'flow_{units}': f'daily_avg_{units}'})
    del df['datetime']

    # create the response
    if return_format == 'csv':
        return v2_utilities.dataframe_to_csv_flask_response(df, f'forecast_anomalies_{reach_id}.csv')
    if return_format == 'json':
        return v2_utilities.dataframe_to_jsonify_response(df=df, df_highres=None, reach_id=reach_id, units=units)
    return df
