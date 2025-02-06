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

def retrospective(
    river_id: int,
    return_format: str,
    start_date: str = None,
    end_date: str = None,
) -> pd.DataFrame:
    """
    Controller for retrieving simulated historic data
    """
    df = geoglows.data.retrospective(river_id, skip_log=True)
    df.columns = df.columns.astype(str)
    df = df.astype(float).round(2)

    if start_date is not None:
        df = df.loc[
            df.index >= datetime.datetime.strptime(start_date, "%Y%m%d")
        ]
    if end_date is not None:
        df = df.loc[df.index <= datetime.datetime.strptime(end_date, "%Y%m%d")]

    if return_format == "csv":
        return df_to_csv_flask_response(df, f"retrospective_{river_id}")
    if return_format == "json":
        return df_to_jsonify_response(df=df, river_id=river_id)
    return df


def daily_averages(river_id: int, return_format: str, bias_corrected: bool = True):
    if bias_corrected:
        df = geoglows.bias.sfdc_bias_correction(river_id)
    else:
        df = geoglows.data.daily_averages(river_id, skip_log=True)
    df.columns = df.columns.astype(str)

    if return_format == "csv":
        return df_to_csv_flask_response(df, f"daily_averages_{river_id}")
    if return_format == "json":
        return df_to_jsonify_response(df=df, river_id=river_id)
    return df


def monthly_averages(river_id: int, return_format: str, bias_corrected: bool = True):
    if bias_corrected:
        df = geoglows.bias.sfdc_bias_correction(621054471).resample("MS").mean()
    else:
        df = geoglows.data.monthly_averages(river_id, skip_log=True)
    df.columns = df.columns.astype(str)
    df = df.astype(float).round(2)

    if return_format == "csv":
        return df_to_csv_flask_response(df, f"monthly_averages_{river_id}")
    if return_format == "json":
        return df_to_jsonify_response(df=df, river_id=river_id)
    return df


def yearly_averages(river_id, return_format, bias_corrected: bool=True):
    if bias_corrected:
        df = geoglows.bias.sfdc_bias_correction(river_id).resample("YS").mean()
    else:
        df = geoglows.data.annual_averages(river_id, skip_log=True)
    df.columns = df.columns.astype(str)
    df = df.astype(float).round(2)

    if return_format == "csv":
        return df_to_csv_flask_response(df, f"yearly_averages_{river_id}")
    if return_format == "json":
        return df_to_jsonify_response(df=df, river_id=river_id)
    return df


def return_periods(river_id: int, return_format: str, bias_corrected: bool = True):
    if bias_corrected:
        df = geoglows.bias.sfdc_bias_correction(river_id)
        annual_max_flow_list = df.groupby(df.index.strftime('%Y')).max().values
        xbar = np.mean(annual_max_flow_list)
        std = np.std(annual_max_flow_list)

        if type(rps) is int:
            rps = (rps,)

        ret_pers = {
            'max_simulated': round(np.max(annual_max_flow_list), 2)
        }
        ret_pers.update({f'return_period_{rp}': round(gumbel1(rp, xbar, std), 2) for rp in rps})
    else:
        ds = xr.open_zarr(PATH_TO_RETURN_PERIODS).sel(rivid=river_id)
        if return_format == "xarray":
            return ds

        df = (
            ds["gumbel1_return_period"]
            .to_dataframe()
            .reset_index()
            .pivot(
                index="rivid",
                columns="return_period",
                values="gumbel1_return_period",
            )
        )
    df.columns = df.columns.astype(str)
    df = df.astype(float).round(2)

    if return_format == "df":
        return df
    if return_format == "csv":
        return df_to_csv_flask_response(df, f"return_periods_{river_id}")
    if return_format == "json":
        return jsonify(
            {
                "return_periods": json.loads(df.to_json(orient="records"))[0],
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
