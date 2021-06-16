import datetime
import glob
import json
import os

import hydrostats.data as hd
import pandas as pd
import xarray
from flask import jsonify

from constants import PATH_TO_ERA_5, M3_TO_FT3
from v2_utilities import get_historical_dataframe, dataframe_to_csv_flask_response, new_json_template
from model_utilities import reach_to_region

__all__ = ['historical', 'historical_averages', 'return_periods']


def historical(reach_id, units, return_format):
    """
    Controller for retrieving simulated historic data
    """
    hist_df = get_historical_dataframe(reach_id, units)

    if return_format == 'csv':
        return dataframe_to_csv_flask_response(hist_df, f'historical_streamflow_era5_{reach_id}.csv')
    if return_format == 'json':
        json_template = new_json_template(reach_id, units, hist_df.index[0], hist_df.index[-1])
        json_template['simulation_forcing'] = "ERA5"
        json_template['time_series'] = {
            'datetime': hist_df.index.tolist(),
            'flow': hist_df[f'flow_{units}'].tolist(),
        }
        return jsonify(json_template)
    if return_format == 'df':
        return hist_df


def historical_averages(reach_id, units, average_type, return_format):
    df = get_historical_dataframe(reach_id, units)
    df.index = pd.to_datetime(df.index)

    if average_type == 'daily':
        df = hd.daily_average(df, rolling=True)
    else:
        df = hd.monthly_average(df)
    df.index.name = 'datetime'

    if return_format == 'csv':
        return dataframe_to_csv_flask_response(df, f'historical_{average_type}_average_{reach_id}.csv')
    if return_format == "json":
        json_template = new_json_template(reach_id, units, df.index[0], df.index[-1])
        json_template['simulation_forcing'] = "ERA5"
        json_template['time_series'] = {
            f'{"month" if average_type == "monthly" else "day_of_year"}': df.index.tolist(),
            'flow': df[f'flow_{units}'].tolist(),
        }
        return jsonify(json_template)
    return df


def return_periods(reach_id, units, return_format):
    region = reach_to_region(reach_id)
    return_period_file = glob.glob(os.path.join(PATH_TO_ERA_5, region, '*return_periods*.nc*'))
    if len(return_period_file) == 0:
        raise ValueError("Unable to find return periods file")

    # collect the data in a dataframe
    return_periods_nc = xarray.open_dataset(return_period_file[0])
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

    if return_format == 'csv':
        return dataframe_to_csv_flask_response(return_periods_df, f'return_periods_{reach_id}.csv')
    if return_format == 'json':
        return jsonify({
            'return_periods': json.loads(return_periods_df.to_json(orient='records'))[0],
            'reach_id': reach_id,
            'simulation_forcing': 'ERA5',
            'gen_date': datetime.datetime.utcnow().strftime('%Y-%m-%dY%X+00:00'),
            'units': {
                'name': 'streamflow',
                'short': f'{units}',
                'long': f'Cubic {"Meters" if units == "cms" else "Feet"} per Second',
            },
        })
