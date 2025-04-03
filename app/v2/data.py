import os
from glob import glob

import natsort
import xarray as xr

from .constants import PATH_TO_FORECASTS, PATH_TO_FORECAST_RECORDS

__all__ = [
    'get_forecast_dataset',
    'find_available_dates',
]


def get_forecast_dataset(river_id: int, date: str) -> xr.Dataset:
    """
    Opens the forecast dataset for a given date, selects the river_id and Qout variable
    """
    if date == "latest":
        date = find_available_dates()[0]

    if len(date) == 8:
        date = f"{date}00"

    #forecast_file = os.path.join(PATH_TO_FORECASTS, f'Qout_{date}.zarr')
    forecast_file = os.path.join(PATH_TO_FORECASTS, f'{date}.zarr')
    
    if not os.path.exists(forecast_file):
        raise ValueError(f'Data not found for date {date}. Use YYYYMMDD format and the AvailableDates endpoint.')
    try:
        forecast_dataset = xr.open_zarr(forecast_file)
    except Exception as e:
        print(e)
        raise ValueError('Error while reading data from the zarr files')
    try:
        return forecast_dataset.sel(rivid=river_id).Qout
    except Exception as e:
        print(e)
        raise ValueError(f'Unable to get data for river_id {river_id} in the forecast dataset')


def get_forecast_records_dataset(vpu: str, year: str):
    """
    Opens the forecast records dataset for a given date, selects the river_id and Qout variable
    """
    forecast_records_file = os.path.join(PATH_TO_FORECAST_RECORDS, f'forecastrecord_{vpu}_{year}.nc')

    if not os.path.exists(forecast_records_file):
        raise ValueError(
            f'Data not found for specified. Use YYYYMMDD format and the Dates endpoint to find valid dates.')
    try:
        forecast_records_dataset = xr.open_dataset(forecast_records_file)
    except Exception as e:
        print(e)
        raise ValueError('Error while reading data from the zarr files')
    return forecast_records_dataset


def find_available_dates() -> list:
    forecast_zarrs = glob(os.path.join(PATH_TO_FORECASTS, "Qout*.zarr"))
    # forecast_zarrs = glob(os.path.join(PATH_TO_FORECASTS, "*.zarr"))
    print(forecast_zarrs)
    forecast_zarrs = natsort.natsorted(forecast_zarrs, reverse=True)
    dates = [os.path.basename(d).replace('.zarr', '').split('_')[1] for d in forecast_zarrs]
    # dates = [os.path.basename(d).replace('.zarr', '') for d in forecast_zarrs]
    return dates
