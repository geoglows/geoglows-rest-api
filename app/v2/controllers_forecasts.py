from datetime import datetime, timedelta

import numpy as np
import pandas as pd
from flask import jsonify

from .constants import NUM_DECIMALS, PACKAGE_METADATA_TABLE_PATH
from .data import (
    get_forecast_dataset,
    get_forecast_records_dataset,
    find_available_dates,
)
from .controllers_historical import return_periods
from .response_formatters import (
    df_to_jsonify_response,
    df_to_csv_flask_response,
    new_json_template,
)

__all__ = [
    "hydroviewer",
    "forecast",
    "forecast_stats",
    "forecast_ensemble",
    "forecast_records",
    "forecast_dates",
]


def hydroviewer(river_id: int, date: str, records_start: str) -> jsonify:
    if date == "latest":
        date = find_available_dates()[-1]
    forecast_df = forecast(river_id, date, "df")

    rperiods = return_periods(river_id, return_format="df")

    # add the columns from the dataframe
    json_template = new_json_template(
        river_id,
        start_date=forecast_df.index[0],
        end_date=forecast_df.index[-1],
    )
    json_template["metadata"]["series"] = (
        ["datetime_forecast", "return_periods"] + forecast_df.columns.tolist()
    )
    
    json_template.update(forecast_df.to_dict(orient="list"))
    json_template.update({"datetime_forecast": forecast_df.index.tolist()})
    json_template["return_periods"] = rperiods.to_dict(orient="records")[0]
    
    if records_start:
        records_df = forecast_records(
            river_id,
            start_date=records_start,
            end_date=date[:8],
            return_format="df",
        )

        try:
            records_df.rename(
                columns={"average_flow": "forecast_records_avg_flow"}, inplace=True
            )
        except Exception as e:
            Exception(f"Forecast records error: {e}.")

        json_template["metadata"]["series"] += records_df.columns.tolist()
        json_template["metadata"]["series"] += ["datetime_records"]
        json_template.update(records_df.to_dict(orient="list"))
        json_template.update({"datetime_records": records_df.index.tolist()})
            
    return jsonify(json_template), 200


def forecast(river_id: int, date: str, return_format: str, bias_corrected: bool = False) -> pd.DataFrame:
    forecast_xarray_dataset = get_forecast_dataset(river_id, date)

    # get an array of all the ensembles, delete the high res before doing averages
    merged_array = forecast_xarray_dataset.data
    merged_array = np.delete(
        merged_array,
        list(forecast_xarray_dataset.ensemble.data).index(52),
        axis=0,
    )

    # replace any values that went negative because of the routing
    merged_array[merged_array <= 0] = 0

    # load all the series into a dataframe
    df = (
        pd.DataFrame(
            {
                f"flow_uncertainty_upper": np.nanpercentile(
                    merged_array, 80, axis=0
                ),
                f"flow_median": np.median(merged_array, axis=0),
                f"flow_uncertainty_lower": np.nanpercentile(
                    merged_array, 20, axis=0
                ),
            },
            index=forecast_xarray_dataset.time.data,
        )
        .dropna()
        .astype(np.float64)
        .round(NUM_DECIMALS)
    )
    df.index = df.index.strftime("%Y-%m-%dT%X+00:00")
    df.index.name = "datetime"

    if return_format == "csv":
        return df_to_csv_flask_response(df, f"forecast_{river_id}")
    if return_format == "json":
        return df_to_jsonify_response(df=df, river_id=river_id)
    return df


def forecast_stats(
    river_id: int, date: str, return_format: str, bias_corrected: bool = False,
) -> pd.DataFrame:
    forecast_xarray_dataset = get_forecast_dataset(river_id, date)

    # get an array of all the ensembles, delete the high res before doing averages
    merged_array = forecast_xarray_dataset.data
    merged_array = np.delete(
        merged_array,
        forecast_xarray_dataset.ensemble.data.tolist().index(52),
        axis=0,
    )

    # replace any values that went negative because of the routing
    merged_array[merged_array <= 0] = 0

    # load all the series into a dataframe
    df = pd.DataFrame(
        {
            f"flow_max": np.amax(merged_array, axis=0),
            f"flow_75p": np.nanpercentile(merged_array, 75, axis=0),
            f"flow_avg": np.mean(merged_array, axis=0),
            f"flow_med": np.median(merged_array, axis=0),
            f"flow_25p": np.nanpercentile(merged_array, 25, axis=0),
            f"flow_min": np.min(merged_array, axis=0),
            f"high_res": forecast_xarray_dataset.sel(ensemble=52).data,
        },
        index=forecast_xarray_dataset.time.data,
    )
    df.index = df.index.strftime("%Y-%m-%dT%X+00:00")
    df.index.name = "datetime"
    df = df.astype(np.float64).round(NUM_DECIMALS)

    if return_format == "csv":
        return df_to_csv_flask_response(df, f"forecast_stats_{river_id}")
    if return_format == "json":
        return df_to_jsonify_response(df=df, river_id=river_id)
    if return_format == "df":
        return df


def forecast_ensemble(river_id: int, date: str, return_format: str, bias_corrected: bool = False):
    forecast_xarray_dataset = get_forecast_dataset(river_id, date)

    # make a list column names (with zero padded numbers) for the pandas DataFrame
    ensemble_column_names = []
    for i in range(1, 53):
        ensemble_column_names.append(f"ensemble_{i:02}")

    # make the data into a pandas dataframe
    df = pd.DataFrame(
        data=np.transpose(forecast_xarray_dataset.data).round(NUM_DECIMALS),
        columns=ensemble_column_names,
        index=forecast_xarray_dataset.time.data,
    )
    df.index = df.index.strftime("%Y-%m-%dT%X+00:00")
    df.index.name = "datetime"
    df = df.astype(np.float64).round(NUM_DECIMALS)

    if return_format == "csv":
        return df_to_csv_flask_response(df, f"forecast_ensemble_{river_id}")
    if return_format == "json":
        return df_to_jsonify_response(df=df, river_id=river_id)
    if return_format == "df":
        return df


def forecast_records(
    river_id: int, start_date: str, end_date: str, return_format: str
) -> pd.DataFrame:
    if start_date is None:
        start_date = datetime.now() - timedelta(days=14)
        start_date = start_date.strftime("%Y%m%d")
    if end_date is None:
        end_date = f"{datetime.now().year + 1}0101"
    year = start_date[:4]

    try:
        start_date = pd.to_datetime(start_date)
        end_date = pd.to_datetime(end_date)
    except ValueError:
        ValueError(
            f"Unrecognized date format for the start_date or end_date. Use YYYYMMDD format."
        )

    metadata_table = pd.read_parquet(
        PACKAGE_METADATA_TABLE_PATH, columns=["LINKNO", "VPUCode"]
    )
    vpu = metadata_table.loc[
        lambda x: x["LINKNO"] == river_id, "VPUCode"
    ].values[0]
    ds = get_forecast_records_dataset(vpu=vpu, year=year)

    # create a dataframe and filter by date
    df = (
        ds.sel(rivid=river_id)
        .Qout.to_dataframe()
        .loc[start_date:end_date]
        .dropna()
        .pivot(columns="rivid", values="Qout")
    )
    df.columns = [
        "average_flow",
    ]
    df["average_flow"] = df["average_flow"].astype(float).round(NUM_DECIMALS)
    df.index = df.index.strftime("%Y-%m-%dT%X+00:00")
    df.index.name = "datetime"

    # create the http response
    if return_format == "csv":
        return df_to_csv_flask_response(df, f"forecast_records_{river_id}")
    if return_format == "json":
        return df_to_jsonify_response(df=df, river_id=river_id)
    if return_format == "df":
        return df


def forecast_dates(return_format: str):
    dates = find_available_dates()
    if return_format == "csv":
        return df_to_csv_flask_response(
            pd.DataFrame(
                dates,
                columns=[
                    "dates",
                ],
            ),
            f"forecast_dates",
            index=False,
        )
    elif return_format == "json":
        return jsonify({"dates": dates})
    else:
        raise ValueError(
            f"Unsupported return format requested: {return_format}"
        )
