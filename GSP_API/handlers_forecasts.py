import os
from datetime import datetime as dt

import numpy as np
import pandas as pd
import xarray
from flask import jsonify, render_template, make_response
from functions import get_units_title, reach_to_region, get_reach_from_latlon, get_region_from_latlon, \
    ecmwf_find_most_current_files

# GLOBAL
PATH_TO_FORECASTS = '/mnt/output/forecasts'
PATH_TO_FORECAST_RECORDS = '/mnt/output/forecast-records'
PATH_TO_ERA_INTERIM = '/mnt/output/era-interim'
PATH_TO_ERA_5 = '/mnt/output/era-5'
M3_TO_FT3 = 35.3146667


def forecast_stats_handler(request):
    """
    Controller that will retrieve forecast statistic data
    in different formats
    """
    reach_id = int(request.args.get('reach_id', False))
    region = request.args.get('region', False)
    lat = request.args.get('lat', '')
    lon = request.args.get('lon', '')
    forecast_folder = request.args.get('date', 'most_recent')
    units = request.args.get('units', 'metric')
    return_format = request.args.get('return_format', 'csv')

    if reach_id:
        # if there wasn't a region provided, try to guess it
        if not region:
            region = reach_to_region(reach_id)
        if not region:
            raise ValueError("Unable to determine a region paired with this reach_id")
    elif lat != '' and lon != '':
        reach_id, region, dist_error = get_reach_from_latlon(lat, lon)
        if dist_error:
            raise ValueError("Unable to find a stream near the lat/lon provided")
    else:
        raise ValueError("Invalid reach_id or lat/lon/region combination")

    # find/check current output datasets
    path_to_output_files = os.path.join(PATH_TO_FORECASTS, region)
    forecast_nc_list, start_date = ecmwf_find_most_current_files(path_to_output_files, forecast_folder)
    forecast_nc_list = sorted(forecast_nc_list)
    if not forecast_nc_list or not start_date:
        raise ValueError(f'ECMWF forecast for region "{region}" and date "{start_date}" not found')

    try:
        # combine 52 ensembles
        qout_datasets = []
        ensemble_index_list = []
        for forecast_nc in forecast_nc_list:
            ensemble_index_list.append(int(os.path.basename(forecast_nc)[:-3].split("_")[-1]))
            qout_datasets.append(xarray.open_dataset(forecast_nc).sel(rivid=reach_id).Qout)
        merged_ds = xarray.concat(qout_datasets, pd.Index(ensemble_index_list, name='ensemble'))

        # get an array of all the ensembles, delete the high res before doing averages
        merged_array = merged_ds.data
        merged_array = np.delete(merged_array, list(merged_ds.ensemble.data).index(52), axis=0)
    except:
        raise ValueError('Error while reading data from the netCDF files')

    # replace any values that went negative because of the muskingham routing
    merged_array[merged_array <= 0] = 0
    short_unit, full_unit = get_units_title(units)

    # load all the series into a dataframe
    df = pd.DataFrame({
        f'flow_max_{short_unit}^3/s': np.amax(merged_array, axis=0),
        f'flow_75%_{short_unit}^3/s': np.percentile(merged_array, 75, axis=0),
        f'flow_avg_{short_unit}^3/s': np.mean(merged_array, axis=0),
        f'flow_25%_{short_unit}^3/s': np.percentile(merged_array, 25, axis=0),
        f'flow_min_{short_unit}^3/s': np.min(merged_array, axis=0),
        f'high_res_{short_unit}^3/s': merged_ds.sel(ensemble=52).data
    }, index=merged_ds.time.data)
    df.index = df.index.strftime('%Y-%m-%dT%H:%M:%SZ')
    df.index.name = 'datetime'

    # handle units conversion
    if short_unit == 'ft':
        for column in df.columns:
            df[column] *= M3_TO_FT3

    if return_format == 'csv':
        response = make_response(df.to_csv())
        response.headers['content-type'] = 'text/csv'
        response.headers['Content-Disposition'] = \
            f'attachment; filename=forecasted_streamflow_{region}_{reach_id}_{short_unit}^3/s.csv'
        return response

    # split the df so that we can use dropna to lists of dates and values without na entries
    high_res_data = df[f'high_res_{short_unit}^3/s'].dropna()
    del df[f'high_res_{short_unit}^3/s']
    df.dropna(inplace=True)

    # create a dictionary with the metadata and series of values
    context = {
        'region': region,
        'comid': reach_id,
        'gendate': dt.utcnow().isoformat() + 'Z',
        'startdate': df.index[0],
        'enddate': df.index[-1],
        'units': {
            'name': 'Streamflow',
            'short': f'{short_unit}^3/s',
            'long': f'Cubic {full_unit} per Second',
        },
        'time_series': {
            'datetime': df.index.tolist(),
            'datetime_high_res': high_res_data.index.tolist(),
            'high_res': high_res_data.to_list(),
        }
    }
    context['time_series'].update(df.to_dict(orient='list'))

    if return_format == "json":
        return jsonify(context)
    # todo fix this
    # elif return_format == "waterml":
    # xml_response = make_response(render_template('forecast_stats.xml', **context))
    # xml_response.headers.set('Content-Type', 'application/xml')
    # return xml_response
    else:
        return jsonify({"error": "Invalid return_format."}), 422


def forecast_ensembles_handler(request):
    """
    Controller that will retrieve forecast ensemble data in different formats
    """
    reach_id = int(request.args.get('reach_id', False))
    ensemble = request.args.get('ensemble', 'all')
    region = request.args.get('region', False)
    lat = request.args.get('lat', '')
    lon = request.args.get('lon', '')
    forecast_folder = request.args.get('date', 'most_recent')
    units = request.args.get('units', 'metric')
    return_format = request.args.get('return_format', 'csv')

    if reach_id:
        # if there wasn't a region provided, try to guess it
        if not region:
            region = reach_to_region(reach_id)
        if not region:
            raise ValueError("Unable to determine a region paired with this reach_id")
    elif lat != '' and lon != '':
        reach_id, region, dist_error = get_reach_from_latlon(lat, lon)
        if dist_error:
            raise ValueError("Unable to find a stream near the lat/lon provided")
    else:
        raise ValueError("Invalid reach_id or lat/lon/region combination")

    # find/check current output datasets
    path_to_output_files = os.path.join(PATH_TO_FORECASTS, region)
    forecast_nc_list, start_date = ecmwf_find_most_current_files(path_to_output_files, forecast_folder)
    forecast_nc_list = sorted(forecast_nc_list)
    if not forecast_nc_list or not start_date:
        raise ValueError(f'ECMWF forecast for region "{region}" and date "{start_date}" not found')

    try:
        # combine 52 ensembles with xarray
        qout_datasets = []
        ensemble_index_list = []
        for forecast_nc in forecast_nc_list:
            ensemble_index_list.append(int(os.path.basename(forecast_nc)[:-3].split("_")[-1]))
            qout_datasets.append(xarray.open_dataset(forecast_nc).sel(rivid=reach_id).Qout)
        merged_ds = xarray.concat(qout_datasets, pd.Index(ensemble_index_list, name='ensemble'))
    except:
        raise ValueError('Error while reading data from the netCDF files')

    short_unit, full_unit = get_units_title(units)

    # make a list column names (with zero padded numbers) for the pandas DataFrame
    ensemble_column_names = []
    for i in ensemble_index_list:
        ensemble_column_names.append(f'ensemble_{i:02}_{short_unit}^3/s')

    # make the data into a pandas dataframe
    df = pd.DataFrame(data=np.transpose(merged_ds.data), columns=ensemble_column_names, index=merged_ds.time.data)
    df.index = df.index.strftime('%Y-%m-%dT%H:%M:%SZ')
    df.index.name = 'datetime'

    # handle units conversion
    if short_unit == 'ft':
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
                    requested_ensembles.append(f'ensemble_{int(i):02}_{short_unit}^3/s')
            else:
                requested_ensembles.append(f'ensemble_{int(ens):02}_{short_unit}^3/s')
        # make a list of columns to remove from the dataframe deleting the requested ens from all ens columns
        for ens in requested_ensembles:
            if ens in ensemble_column_names:
                ensemble_column_names.remove(ens)
        # delete the dataframe columns we aren't interested
        for ens in ensemble_column_names:
            del df[ens]

    if return_format == 'csv':
        response = make_response(df.to_csv())
        response.headers['content-type'] = 'text/csv'
        response.headers['Content-Disposition'] = \
            f'attachment; filename=forecasted_ensembles_{region}_{reach_id}_{short_unit}^3/s.csv'
        return response

    # for any column in the dataframe (e.g. each ensemble)
    ensemble_ts_dict = {
        'datetime': df[f'ensemble_01_{short_unit}^3/s'].dropna(inplace=False).index.tolist(),
        'datetime_high_res': df[f'ensemble_52_{short_unit}^3/s'].dropna(inplace=False).index.tolist(),
    }
    for column in df.columns:
        ensemble_ts_dict[column] = df[column].dropna(inplace=False).tolist()

    context = {
        'region': region,
        'comid': reach_id,
        'startdate': df.index[0],
        'enddate': df.index[-1],
        'gendate': dt.utcnow().isoformat() + 'Z',
        'time_series': ensemble_ts_dict,
        'units': {
            'name': 'Streamflow',
            'short': f'{short_unit}3/s',
            'long': f'Cubic {full_unit} per Second'
        }
    }

    if return_format == 'json':
        return jsonify(context)

    if return_format == 'waterml':
        xml_response = make_response(render_template('forecast_ensembles.xml', **context))
        xml_response.headers.set('Content-Type', 'application/xml')

        return xml_response

    else:
        return jsonify({"error": "Invalid return_format."}), 422


def forecast_warnings_handler(request):
    region = request.args.get('region', False)
    lat = request.args.get('lat', False)
    lon = request.args.get('lon', False)
    forecast_date = request.args.get('forecast_date', 'most_recent')
    return_format = request.args.get('return_format', 'csv')

    if not region:
        if lat and lon:
            region = get_region_from_latlon(lat, lon)
        else:
            return {"error": 'Provide a valid latitude and longitude'}

    # find/check current output datasets
    path_to_region_forecasts = os.path.join(PATH_TO_FORECASTS, region)
    if forecast_date == 'most_recent':
        date_folders = sorted(
            [d for d in os.listdir(path_to_region_forecasts)
             if os.path.isdir(os.path.join(path_to_region_forecasts, d))],
            reverse=True
        )
        folder = os.path.join(path_to_region_forecasts, date_folders[0])
    else:
        folder = os.path.join(path_to_region_forecasts, forecast_date)
        if not os.path.isdir(folder):
            return {"error": f'Forecast date {forecast_date} was not found'}

    # locate the forecast warning csv
    summary_file = os.path.join(folder, 'forecasted_return_periods_summary.csv')

    if not os.path.isfile(summary_file):
        return {"error": "summary file was not found for this region and forecast date"}

    warning_summary = pd.read_csv(summary_file)

    if return_format == 'csv':
        response = make_response(warning_summary.to_csv(index=False))
        response.headers['content-type'] = 'text/csv'
        response.headers['Content-Disposition'] = f'attachment; filename=ForecastWarnings-{region}.csv'
        return response

    elif return_format == 'json':
        warning_summary.index = warning_summary['comid']
        del warning_summary['comid']
        return jsonify(warning_summary.to_dict(orient='index'))


def forecast_records_handler(request):
    reach_id = int(request.args.get('reach_id', False))
    lat = request.args.get('lat', '')
    lon = request.args.get('lon', '')
    units = request.args.get('units', 'metric')
    return_format = request.args.get('return_format', 'csv')

    year = dt.utcnow().year
    start_date = request.args.get('start_date', dt(year=year, month=1, day=1).strftime('%Y%m%d'))
    end_date = request.args.get('end_date', dt(year=year, month=12, day=31).strftime('%Y%m%d'))

    # determine if you have a reach_id and region from the inputs
    if reach_id:
        region = reach_to_region(reach_id)
        if not region:
            return jsonify({"error": "Unable to determine a region paired with this reach_id"}, 422)
    elif lat != '' and lon != '':
        reach_id, region, dist_error = get_reach_from_latlon(lat, lon)
        if dist_error:
            return jsonify(dist_error)
    else:
        return jsonify({"error": "Invalid reach_id or lat/lon/region combination"}, 422)

    # validate the times
    try:
        start_date = dt.strptime(start_date, '%Y%m%d')
        end_date = dt.strptime(end_date, '%Y%m%d')
    except:
        return jsonify({'Error': 'Unrecognized start_date or end_date. Use YYYYMMDD format'})

    # open and read the forecast record netcdf
    record_path = os.path.join(PATH_TO_FORECAST_RECORDS, region, f'forecast_record-{year}-{region}.nc')
    forecast_record = xarray.open_dataset(record_path)
    times = pd.to_datetime(pd.Series(forecast_record['time'].data, name='datetime'), unit='s', origin='unix')
    record_flows = forecast_record.sel(rivid=reach_id)['Qout']
    forecast_record.close()
    short_unit, full_unit = get_units_title(units)

    # create a dataframe and filter by date
    df = times.to_frame().join(pd.Series(record_flows, name=f'streamflow_{short_unit}^3/s'))
    df = df[df['datetime'].between(start_date, end_date)]
    df.index = df['datetime']
    del df['datetime']
    df.index = df.index.strftime('%Y-%m-%dT%H:%M:%SZ')
    df.index.name = 'datetime'
    df[df[f'streamflow_{short_unit}^3/s'] > 1000000000] = np.nan
    df.dropna(inplace=True)
    if units == 'english':
        df[f'streamflow_{short_unit}^3/s'] *= M3_TO_FT3

    # create the http response
    if return_format == 'csv':
        response = make_response(df.to_csv())
        response.headers['content-type'] = 'text/csv'
        response.headers['Content-Disposition'] = f'attachment; filename=forecast_record_{reach_id}.csv'
        return response

    elif return_format == 'json':
        return {
            'region': region,
            'comid': reach_id,
            'gendate': dt.utcnow().isoformat() + 'Z',
            'startdate': df.index[0],
            'enddate': df.index[-1],
            'units': {
                'name': 'Streamflow',
                'short': f'{short_unit}^3/s',
                'long': f'Cubic {full_unit} per Second',
            },
            'time_series': {
                'datetime': df.index.tolist(),
                'flow': df[f'streamflow_{short_unit}^3/s'].tolist(),
            }
        }


def available_dates_handler(request):
    """
    Controller that returns available dates.
    """
    region = request.args.get('region', '')

    region_path = os.path.join(PATH_TO_FORECASTS, region)

    if not os.path.exists(region_path):
        return jsonify({"message": "Region does not exist."})

    dates = [d for d in os.listdir(region_path) if d.split('.')[0].isdigit()]

    if len(dates) > 0:
        return jsonify({"available_dates": dates})
    else:
        return jsonify({"message": "No dates available."}), 204
