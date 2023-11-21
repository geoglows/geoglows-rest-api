import os
from glob import glob

import natsort
import pandas as pd
import xarray as xr

from .constants import PATH_TO_FORECASTS, PATH_TO_ERA_5, M3_TO_FT3

__all__ = [
    'get_forecast_dataset',
    'get_historical_dataframe',
    'get_return_periods_dataframe',
    'find_available_dates',
    'find_forecast_warnings',
    'latlon_to_reach',
]


def get_forecast_dataset(reach_id: int, date: str) -> xr.Dataset:
    """
    Opens the forecast dataset for a given date, selects the reach_id and Qout variable
    """
    if date == "latest":
        date = find_available_dates()[-1]

    forecast_file = os.path.join(PATH_TO_FORECASTS, f'Qout_{date}00.zarr')

    if not os.path.exists(forecast_file):
        raise ValueError(f'Data not found for date {date}. Use YYYYMMDD format and the AvailableDates endpoint.')

    try:
        forecast_dataset = xr.open_zarr(forecast_file)
    except Exception as e:
        print(e)
        raise ValueError('Error while reading data from the zarr files')
    try:
        return forecast_dataset.sel(rivid=reach_id).Qout
    except Exception as e:
        print(e)
        raise ValueError(f'Unable to get data for reach_id {reach_id} in the forecast dataset')


def get_historical_dataframe(reach_id, units):
    # todo
    print("reading historical data")
    print("reach_id: {reach_id}")
    region = 'central_america-geoglows'  # reach_to_region(reach_id)
    print("region {region}")
    file_path_list = glob(os.path.join(PATH_TO_ERA_5, region, 'Qout*.zarr'))

    if len(file_path_list) == 0:
        raise ValueError('historical data not found')

    historical_data_file = xr.open_zarr(file_path_list[0])
    time = pd.to_datetime(historical_data_file['time'][:], unit='s')
    df = pd.DataFrame()
    try:
        df['flow'] = historical_data_file['Qout'][:, list(historical_data_file['rivid'][:]).index(reach_id)]
        df.set_index(time, inplace=True)
    except Exception as e:
        # qout_nc.close()
        raise e

    if units == 'cfs':
        df['flow'] = df['flow'].values * M3_TO_FT3
    df.rename(columns={'flow': f'flow_{units}'}, inplace=True)
    print("finished reading historical data")
    return df


def get_return_periods_dataframe(reach_id: int, units: str) -> pd.DataFrame:
    # todo
    # region = reach_to_region(reach_id)
    region = ''
    return_period_file = glob(os.path.join(PATH_TO_ERA_5, region, '*return_periods*.nc*'))
    if len(return_period_file) == 0:
        raise ValueError("Unable to find return periods file")

    # collect the data in a dataframe
    return_periods_nc = xr.open_dataset(return_period_file[0])
    return_periods_df = return_periods_nc.to_dataframe()
    return_periods_df = return_periods_df[return_periods_df.index == reach_id]
    return_periods_nc.close()

    try:
        del return_periods_df['lon'], return_periods_df['lat']
    except Exception:
        pass

    if units == 'cfs':
        for column in return_periods_df:
            return_periods_df[column] *= M3_TO_FT3
    return return_periods_df


def find_available_dates() -> list:
    forecast_zarrs = glob(os.path.join(PATH_TO_FORECASTS, "Qout*.zarr"))
    forecast_zarrs = natsort.natsorted(forecast_zarrs, reverse=True)
    dates = [os.path.basename(d).replace('.zarr', '').split('_')[1] for d in forecast_zarrs]
    return dates


def find_forecast_warnings(date) -> pd.DataFrame:
    warnings = None

    for region in os.listdir(PATH_TO_FORECASTS):
        # find/check current output datasets
        path_to_region_forecasts = os.path.join(PATH_TO_FORECASTS, region)
        if not os.path.isdir(path_to_region_forecasts):
            continue
        if date == 'most_recent':
            date_folders = sorted([d for d in os.listdir(path_to_region_forecasts)
                                   if os.path.isdir(os.path.join(path_to_region_forecasts, d))],
                                  reverse=True)
            folder = os.path.join(path_to_region_forecasts, date_folders[0])
        else:
            folder = os.path.join(path_to_region_forecasts, date)
            if not os.path.isdir(folder):
                raise ValueError(f'Forecast date {date} was not found')
        # locate the forecast warning csv
        summary_file = os.path.join(folder, 'forecasted_return_periods_summary.csv')
        region_warnings_df = pd.read_csv(summary_file)
        region_warnings_df['region'] = region.replace('-geoglows', '')
        if not os.path.isfile(summary_file):
            continue
        if warnings is None:
            warnings = region_warnings_df
            continue

        warnings = pd.concat([warnings, region_warnings_df], axis=0)

    if warnings is None:
        raise ValueError('Unable to find any warnings csv files for any region for the specified date')

    return warnings


def latlon_to_reach(lat: float, lon: float) -> list:
    """
    Finds the reach ID nearest to a given lat/lon
    Uses the ModelMasterTable to find the locations
    """
    df = pd.read_parquet('/mnt/configs/geoglows-v2-master-table.parquet', columns=['TDXHydroLinkNo', 'lat', 'lon'])
    df['distance'] = ((df['lat'] - lat) ** 2 + (df['lon'] - lon) ** 2) ** 0.5
    return df.sort_values('distance').reset_index(drop=True).iloc[0][['TDXHydroLinkNo', 'distance']].values.flatten()

