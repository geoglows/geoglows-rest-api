import datetime
import json

import geoglows
import pandas as pd
from flask import jsonify

import numpy as np
import math

import xarray as xr

from .response_formatters import (
    df_to_csv_flask_response,
    df_to_jsonify_response,
)

from .constants import PATH_TO_RETURN_PERIODS
__all__ = [
    "retrospective",
    "daily_averages",
    "monthly_averages",
    "yearly_averages",
    "return_periods",
]

def gumbel1(rp: int, xbar: float, std: float) -> float:
    """
    Solves the Gumbel Type 1 distribution
    Args:
        rp: return period (years)
        xbar: average of the dataset
        std: standard deviation of the dataset

    Returns:
        float: solution to gumbel distribution
    """
    return round(-math.log(-math.log(1 - (1 / rp))) * std * .7797 + xbar - (.45 * std), 2)

def retrospective_daily(
    river_id: int,
    return_format: str,
    start_date: str = None,
    end_date: str = None,
    bias_corrected: bool = False,
) -> pd.DataFrame:
    """
    Controller for retrieving simulated historic data
    """
    if bias_corrected:
        sim_data = geoglows.data.retro_daily(river_id, skip_log=True)
        df = geoglows.bias.sfdc_bias_correction(sim_data, river_id)
        df[f"{river_id}_original"] = sim_data[river_id]
    else:
        df = geoglows.data.retro_daily(river_id, skip_log=True)
    df.columns = df.columns.astype(str)
    df = df.astype(float).round(2)

    if start_date is not None:
        start_dt = datetime.datetime.strptime(start_date, "%Y%m%d").replace(tzinfo=datetime.timezone.utc)
        df = df.loc[df.index >= start_dt]

    if end_date is not None:
        end_dt = datetime.datetime.strptime(end_date, "%Y%m%d").replace(tzinfo=datetime.timezone.utc)
        df = df.loc[df.index <= end_dt]

    if return_format == "csv":
        return df_to_csv_flask_response(df, f"retrospective_{river_id}")
    if return_format == "json":
        return df_to_jsonify_response(df=df, river_id=river_id)
    return df

def retrospective_hourly(
    river_id: int,
    return_format: str,
    start_date: str = None,
    end_date: str = None,
) -> pd.DataFrame:
    """
    Controller for retrieving simulated historic data
    """
    df = geoglows.data.retro_hourly(river_id, skip_log=True)
    df.columns = df.columns.astype(str)
    df = df.astype(float).round(2)

    if start_date is not None:
        start_dt = datetime.datetime.strptime(start_date, "%Y%m%d").replace(tzinfo=datetime.timezone.utc)
        df = df.loc[df.index >= start_dt]

    if end_date is not None:
        end_dt = datetime.datetime.strptime(end_date, "%Y%m%d").replace(tzinfo=datetime.timezone.utc)
        df = df.loc[df.index <= end_dt]

    if return_format == "csv":
        return df_to_csv_flask_response(df, f"retrospective_{river_id}")
    if return_format == "json":
        return df_to_jsonify_response(df=df, river_id=river_id)
    return df

def retrospective_monthly(
    river_id: int,
    return_format: str,
    start_date: str = None,
    end_date: str = None,
    bias_corrected: bool = False,
) -> pd.DataFrame:
    """
    Controller for retrieving simulated historic data
    """
    if bias_corrected:
        sim_data = geoglows.data.retro_daily(river_id, skip_log=True)
        df = geoglows.bias.sfdc_bias_correction(sim_data, river_id).resample("MS").mean()
        df[f"{river_id}_original"] = sim_data[river_id]
    else:
        df = geoglows.data.retro_monthly(river_id, skip_log=True)
    df.columns = df.columns.astype(str)
    df = df.astype(float).round(2)

    if start_date is not None:
        start_dt = datetime.datetime.strptime(start_date, "%Y%m%d").replace(tzinfo=datetime.timezone.utc)
        df = df.loc[df.index >= start_dt]

    if end_date is not None:
        end_dt = datetime.datetime.strptime(end_date, "%Y%m%d").replace(tzinfo=datetime.timezone.utc)
        df = df.loc[df.index <= end_dt]

    if return_format == "csv":
        return df_to_csv_flask_response(df, f"retrospective_{river_id}")
    if return_format == "json":
        return df_to_jsonify_response(df=df, river_id=river_id)
    return df


def daily_averages(river_id: int, return_format: str, bias_corrected: bool = False):
    if bias_corrected:
        sim_data = geoglows.data.retro_daily(river_id, skip_log=True)
        data = geoglows.bias.sfdc_bias_correction(sim_data, river_id)
        data[f"{river_id}_original"] = sim_data[river_id]
    else:
        data = geoglows.data.retro_daily(river_id)
    df = data.groupby([data.index.month, data.index.day]).mean()
    df.index = df.index.map(lambda x: f"{x[0]:02d}-{x[1]:02d}")
    df.columns = df.columns.astype(str)
    if return_format == "csv":
        return df_to_csv_flask_response(df, f"daily_averages_{river_id}")
    if return_format == "json":
        return df_to_jsonify_response(df=df, river_id=river_id)
    return df


def monthly_averages(river_id: int, return_format: str, bias_corrected: bool = False):
    if bias_corrected:
        sim_data = geoglows.data.retro_daily(river_id, skip_log=True)
        data = geoglows.bias.sfdc_bias_correction(sim_data, river_id).resample("MS").mean()
        data[f"{river_id}_original"] = sim_data[river_id]
    else:
        data = geoglows.data.retro_monthly(river_id)
    df = data.groupby(data.index.month).mean()
    df.columns = df.columns.astype(str)
    df = df.astype(float).round(2)

    if return_format == "csv":
        return df_to_csv_flask_response(df, f"monthly_averages_{river_id}")
    if return_format == "json":
        return df_to_jsonify_response(df=df, river_id=river_id)
    return df


def yearly_averages(river_id, return_format, bias_corrected: bool = False):
    if bias_corrected:
        sim_data = geoglows.data.retro_daily(river_id, skip_log=True)
        df = geoglows.bias.sfdc_bias_correction(sim_data, river_id).resample("YS").mean()
        df[f"{river_id}_original"] = sim_data[river_id]
    else:
        df = geoglows.data.retro_yearly(river_id)
    df.columns = df.columns.astype(str)
    df = df.astype(float).round(2)

    if return_format == "csv":
        return df_to_csv_flask_response(df, f"yearly_averages_{river_id}")
    if return_format == "json":
        return df_to_jsonify_response(df=df, river_id=river_id)
    return df


def return_periods(river_id: int, return_format: str, bias_corrected: bool = False):
    if bias_corrected:
        sim_data = geoglows.data.retro_daily(river_id)
        df = geoglows.bias.sfdc_bias_correction(sim_data = sim_data, river_id=river_id)
        rps = [2, 5, 10, 25, 50, 100]
        results = []
        df = df.rename(columns={str(river_id): 'return_periods'})
        df['return_periods_original'] = sim_data[river_id]
        for column in ["return_periods_original", "return_periods"]:
            annual_max_flow_list = df.groupby(df.index.strftime('%Y'))[column].max().values.flatten()
            xbar = np.mean(annual_max_flow_list)
            std = np.std(annual_max_flow_list)

            # Compute return periods
            ret_pers = {'Data Type': column, 'max_simulated': round(np.max(annual_max_flow_list), 2)}
            ret_pers.update({f'{rp}': round(gumbel1(rp, xbar, std), 2) for rp in rps})
            
            results.append(ret_pers)
        df = (pd.DataFrame(results).set_index("Data Type")).transpose()
        df.columns = df.columns.astype(str)
        df = df.astype(float).round(2)
        if return_format == "json":
            return jsonify(
                {
                    "return_periods_original": df["return_periods_original"].to_dict(),
                    "return_periods": df["return_periods"].to_dict(),
                    "river_id": river_id,
                    "gen_date": datetime.datetime.now(datetime.UTC).strftime(
                        "%Y-%m-%dT%X+00:00"
                    ),
                    "units": {
                        "name": "streamflow",
                        "short": "cms",
                        "long": "cubic meters per second",
                    },
                }
            )
    else:
        df =  geoglows.data.return_periods(river_id)
        df.columns = df.columns.astype(str)
        df = df.astype(float).round(2)
        if return_format == "json":
            return jsonify(
                {
                    "return_periods": df.squeeze().to_dict(),
                    "river_id": river_id,
                    "gen_date": datetime.datetime.now(datetime.UTC).strftime(
                        "%Y-%m-%dT%X+00:00"
                    ),
                    "units": {
                        "name": "streamflow",
                        "short": "cms",
                        "long": "cubic meters per second",
                    },
                }
            )
    if return_format == "csv":
        return df_to_csv_flask_response(df, f"return_periods_{river_id}")
