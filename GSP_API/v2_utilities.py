import os
import pandas as pd
import xarray
import datetime
import glob
import netCDF4 as nc
import numpy as np

from flask import make_response, jsonify

from constants import PATH_TO_FORECASTS, PATH_TO_ERA_5, M3_TO_FT3
from v1_functions import reach_to_region, latlon_to_reach


def handle_request(request, product, reach_id, return_format):
    products = (
        'forecast', 'forecaststats', 'forecastensembles', 'forecastwarnings', 'forecastrecords', 'forecastanomalies',
        'historical', 'hindcast', 'returnperiods', 'dailyaverages', 'monthlyaverages',
    )
    data_units = ('cms', 'cfs',)
    return_formats = ('csv', 'json',)

    product = str(product).lower()
    if product not in products:
        raise ValueError(f'{product} not recognized. available data products are: {products}')

    if reach_id is None:
        reach_id = latlon_to_reach(request.args.get('lat', None), request.args.get('lon', None))
    try:
        reach_id = int(reach_id)
    except Exception:
        raise ValueError("reach_id should be an integer corresponding to a valid ID of a stream segment")

    if return_format not in return_formats:
        raise ValueError('format not recognized. must be either "json" or "csv"')

    units = request.args.get('units', 'cms')
    if units not in data_units:
        raise ValueError(f'units not recognized, choose from: {data_units}')

    year = datetime.datetime.utcnow().year
    date = request.args.get('date', 'latest')
    start_date = request.args.get('start_date', datetime.datetime(year=year, month=1, day=1).strftime('%Y%m%d'))
    end_date = request.args.get('end_date', datetime.datetime(year=year, month=12, day=31).strftime('%Y%m%d'))

    ensemble = request.args.get('ensemble', 'all')

    return product, reach_id, return_format, units, date, ensemble, start_date, end_date


def get_forecast_dataset(reach_id, date):
    region = reach_to_region(reach_id)
    region_forecast_dir = os.path.join(PATH_TO_FORECASTS, region)

    if date == "latest":
        directory_of_forecast_data = sorted(
            [os.path.join(region_forecast_dir, d) for d in os.listdir(region_forecast_dir)
             if os.path.isdir(os.path.join(region_forecast_dir, d))],
            reverse=True
        )
        if len(directory_of_forecast_data) > 0:
            directory_of_forecast_data = directory_of_forecast_data[0]
    else:
        directory_of_forecast_data = os.path.join(region_forecast_dir, date)

    if not os.path.exists(directory_of_forecast_data):
        raise ValueError(f'forecast data not found for region "{region}" and date {date}')

    forecast_nc_list = sorted(glob.glob(os.path.join(directory_of_forecast_data, "Qout*.nc")), reverse=True)

    if len(forecast_nc_list) == 0:
        raise ValueError('forecast data not found')

    try:
        # combine 52 ensembles
        qout_datasets = []
        ensemble_index_list = []
        for forecast_nc in forecast_nc_list:
            ensemble_index_list.append(int(os.path.basename(forecast_nc)[:-3].split("_")[-1]))
            qout_datasets.append(xarray.open_dataset(forecast_nc).sel(rivid=reach_id).Qout)
        return xarray.concat(qout_datasets, pd.Index(ensemble_index_list, name='ensemble'))
    except Exception as e:
        print(e)
        raise ValueError('Error while reading data from the netCDF files')


def dataframe_to_csv_flask_response(df: pd.DataFrame, csv_name: str):
    response = make_response(df.to_csv())
    response.headers['content-type'] = 'text/csv'
    response.headers['Content-Disposition'] = f'attachment; filename={csv_name}.csv'
    return response


def dataframe_to_jsonify_response(df: pd.DataFrame, reach_id: int, units: str):
    json_template = new_json_template(reach_id, units, start_date=df.index[0], end_date=df.index[-1])

    # add the columns from the dataframe
    json_template['datetime'] = df.index.tolist()
    json_template.update(df.replace(np.nan, '').to_dict(orient='list'))
    json_template['metadata']['series'] = df.columns.tolist()

    return jsonify(json_template)


def new_json_template(reach_id, units, start_date, end_date):
    return {
        'metadata': {
            'reach_id': reach_id,
            'gen_date': datetime.datetime.utcnow().strftime('%Y-%m-%dY%X+00:00'),
            'start_date': start_date,
            'end_date': end_date,
            'series': [],
            'units': {
                'name': 'streamflow',
                'short': f'{units}',
                'long': f'Cubic {"Meters" if units == "cms" else "Feet"} per Second',
            },
        }
    }


def get_historical_dataframe(reach_id: int, units: str) -> pd.DataFrame:
    region = reach_to_region(reach_id)
    historical_data_file = glob.glob(os.path.join(PATH_TO_ERA_5, region, 'Qout*.nc*'))[0]
    template = os.path.join(PATH_TO_ERA_5, 'era5_pandas_dataframe_template.pickle')

    # collect the data in a dataframe
    df = pd.read_pickle(template)
    qout_nc = nc.Dataset(historical_data_file)
    try:
        df['flow'] = qout_nc['Qout'][:, list(qout_nc['rivid'][:]).index(reach_id)]
        qout_nc.close()
    except Exception as e:
        qout_nc.close()
        raise e

    if units == 'cfs':
        df['flow'] = df['flow'].values * M3_TO_FT3
    df.rename(columns={'flow': f'flow_{units}'}, inplace=True)

    return df
