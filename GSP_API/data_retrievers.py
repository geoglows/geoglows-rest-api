import pandas as pd
import xarray
import netCDF4 as nc
import numpy as np
import os

from constants import PATH_TO_FORECASTS, M3_TO_FT3
from functions import ecmwf_find_most_current_files, find_historical_files, get_units_title


def get_forecast_stats_dataframe(reach_id: int, region: str, date: str, units: str):
    # find/check current output datasets
    path_to_output_files = os.path.join(PATH_TO_FORECASTS, region)
    forecast_nc_list, start_date = ecmwf_find_most_current_files(path_to_output_files, date)
    forecast_nc_list = sorted(forecast_nc_list)
    if not forecast_nc_list or not start_date:
        raise ValueError(f'ECMWF forecast for region "{region}" and date "{start_date}" not found')

    try:
        # combine ensembles
        qout_datasets = []
        ensemble_index_list = []
        for forecast_nc in forecast_nc_list:
            ensemble_index_list.append(int(os.path.basename(forecast_nc)[:-3].split("_")[-1]))
            qout_datasets.append(xarray.open_dataset(forecast_nc).sel(rivid=reach_id).Qout)
        merged_ds = xarray.concat(qout_datasets, pd.Index(ensemble_index_list, name='ensemble'))

        # get an array of all the ensembles, delete the high res before doing averages
        merged_array = merged_ds.data
        merged_array = np.delete(merged_array, list(merged_ds.ensemble.data).index(52), axis=0)
    except Exception as e:
        raise ValueError(f'Error while reading data from the netCDF files: {e}')

    # replace any negative values created by RAPID
    merged_array[merged_array <= 0] = 0

    # load all the series into a dataframe
    df = pd.DataFrame({
        f'flow_max_{units}': np.amax(merged_array, axis=0),
        f'flow_75%_{units}': np.percentile(merged_array, 75, axis=0),
        f'flow_avg_{units}': np.mean(merged_array, axis=0),
        f'flow_25%_{units}': np.percentile(merged_array, 25, axis=0),
        f'flow_min_{units}': np.min(merged_array, axis=0),
        f'high_res_{units}': merged_ds.sel(ensemble=52).data
    }, index=merged_ds.time.data)
    df.index = df.index.strftime('%Y-%m-%dT%H:%M:%SZ')
    df.index.name = 'datetime'

    return df


def get_historical_dataframe(reach_id, region, units, forcing):
    historical_data_file, template = find_historical_files(region, forcing)

    # handle the units
    units_title, units_title_long = get_units_title(units)

    # collect the data in a dataframe
    df = pd.read_pickle(template)
    qout_nc = nc.Dataset(historical_data_file)
    try:
        df['flow'] = qout_nc['Qout'][:, list(qout_nc['rivid'][:]).index(reach_id)]
        qout_nc.close()
    except Exception as e:
        qout_nc.close()
        raise e
    if units == 'english':
        df['flow'] = df['flow'].values * M3_TO_FT3
    df.rename(columns={'flow': f'streamflow_{units_title}^3/s'}, inplace=True)
    return df
