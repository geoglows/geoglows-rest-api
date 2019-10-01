import os
from csv import writer as csv_writer
from glob import glob
from io import StringIO

import pandas as pd
import xarray
from flask import make_response
from functions import ecmwf_find_most_current_files, M3_TO_FT3, get_units_title, reach_to_region
from shapely.geometry import Point, MultiPoint
from shapely.ops import nearest_points

# GLOBAL
PATH_TO_HISTORICAL = '/mnt/output/era'
PATH_TO_FORECASTS = '/mnt/output/ecmwf'


def get_forecast_streamflow_csv(request):
    """
    Retrieve the forecasted streamflow as CSV
    """

    try:
        # retrieve statistics
        forecast_statistics, region, river_id, units = get_ecmwf_forecast_statistics(request)

        # prepare to write response for CSV
        si = StringIO()
        writer = csv_writer(si)

        forecast_df = pd.DataFrame(forecast_statistics)
        column_names = (forecast_df.columns.values + [' ({}3/s)'.format(get_units_title(units))]).tolist()
        writer.writerow(['datetime'] + column_names)

        for row_data in forecast_df.itertuples():
            writer.writerow(row_data)

        response = make_response(si.getvalue())
        response.headers['content-type'] = 'text/csv'
        response.headers['Content-Disposition'] = \
            'attachment; filename=forecasted_streamflow_{0}_{1}.csv'.format(region, river_id)

        return response

    except:
        return {"error": "An unexpected error occurred with the CSV response."}, 422
    

def get_forecast_ensemble_csv(request):
    """
    Retrieve the forecast ensemble as CSV
    """
    try:
        # retrieve ensembles
        forecast_statistics, region, river_id, units = get_ecmwf_ensemble(request)

        # prepare to write response for CSV
        si = StringIO()
        writer = csv_writer(si)

        forecast_df = pd.DataFrame(forecast_statistics)
        column_names = (forecast_df.columns.values + [' ({}3/s)'.format(get_units_title(units))]).tolist()

        writer.writerow(['datetime'] + column_names)

        for row_data in forecast_df.itertuples():
            writer.writerow(row_data)

        # prepare to write response for CSV
        response = make_response(si.getvalue())
        response.headers['content-type'] = 'text/csv'
        response.headers['Content-Disposition'] = \
            'attachment; filename=forecasted_ensembles_{0}_{1}.csv'.format(region, river_id)

        return response
    except:
        return {"error": "An unexpected error occured with the CSV response."}, 422


def get_historic_data_csv(request):
    """""
    Returns ERA Interim data as csv
    """""

    try:
        qout_data, river_id, region, units = get_historic_streamflow_series(request)

        si = StringIO()
        writer = csv_writer(si)

        writer.writerow(['datetime', 'streamflow ({}3/s)'.format(get_units_title(units))])

        for row_data in qout_data.items():
            writer.writerow(row_data)

        # prepare to write response
        response = make_response(si.getvalue())
        response.headers['content-type'] = 'text/csv'
        response.headers['Content-Disposition'] = \
            'attachment; filename=historic_streamflow_{0}_{1}.csv'.format(region, river_id)

        return response
    except:
        return {"error": "An unexpected error occured with the CSV response."}, 422


def get_seasonal_avg_csv(request):
    """""
    Returns seasonal data as csv
    """""

    try:
        qout_data, river_id, region, units = get_seasonal_average(request)

        si = StringIO()
        writer = csv_writer(si)

        writer.writerow(['day', 'streamflow_avg ({}3/s)'.format(get_units_title(units))])

        for row_data in qout_data.items():
            writer.writerow(row_data)

        # prepare to write response
        response = make_response(si.getvalue())
        response.headers['content-type'] = 'text/csv'
        response.headers['Content-Disposition'] = \
            'attachment; filename=seasonal_streamflow_average_{0}_{1}.csv'.format(region, river_id)

        return response
    except:
        return {"error": "An unexpected error occured with the CSV response."}, 422


def get_return_period_csv(request):
    """""
    Returns ERA Interim data as csv
    """""

    try:
        return_period_data, river_id, region, units = get_return_period_dict(request)

        si = StringIO()
        writer = csv_writer(si)

        writer.writerow(['return period', 'streamflow ({}3/s)'.format(get_units_title(units))])

        for key, value in return_period_data.items():
            writer.writerow([key, value])

        # prepare to write response
        response = make_response(si.getvalue())
        response.headers['content-type'] = 'text/csv'
        response.headers['Content-Disposition'] = \
            'attachment; filename=return_periods_{0}_{1}.csv'.format(region, river_id)

        return response
    except:
        return {"error": "An unexpected error occured with the CSV response."}, 422


def get_ecmwf_forecast_statistics(request):
    """
    Returns the statistics for the 52 member forecast
    """
    reach_id = int(request.args.get('reach_id', False))
    region = request.args.get('region', '')
    stat_type = request.args.get('stat', '')
    lat = request.args.get('lat', '')
    lon = request.args.get('lon', '')
    forecast_folder = request.args.get('date', 'most_recent')
    units = request.args.get('units', 'metric')

    if reach_id:
        region = reach_to_region(reach_id)
        if not region:
            return {"error": "Unable to determine a region paired with this reach_id"}
    elif lat != '' and lon != '' and region != '':
        point = Point(float(lat), float(lon))
        df = pd.read_csv(
            f"/app/GSP_API/region_coordinate_files/{region}/comid_lat_lon_z.csv",
            sep=',',
            header=0,
            index_col=0
        )

        points_df = df.loc[:, "Lat":"Lon"].apply(Point, axis=1)
        multi_pt = MultiPoint(points_df.tolist())

        nearest_pt = nearest_points(point, multi_pt)
        reach_id = int(points_df[points_df == nearest_pt[1]].index[0])

        if nearest_pt[0].distance(nearest_pt[1]) > 0.11:
            return {"error": "Nearest river is more than ~10km away."}
    else:
        return {"error": "Invalid reach_id or lat/lon/region combination"}, 422

    # find/check current output datasets
    path_to_output_files = os.path.join(PATH_TO_FORECASTS, region)
    forecast_nc_list, start_date = ecmwf_find_most_current_files(path_to_output_files, forecast_folder)
    if not forecast_nc_list or not start_date:
        return {"error": 'ECMWF forecast for ' + region}, 422

    # combine 52 ensembles
    qout_datasets = []
    ensemble_index_list = []
    for forecast_nc in forecast_nc_list:
        ensemble_index_list.append(int(os.path.basename(forecast_nc)[:-3].split("_")[-1]))
        qout_datasets.append(xarray.open_dataset(forecast_nc).sel(rivid=reach_id).Qout)

    merged_ds = xarray.concat(qout_datasets, pd.Index(ensemble_index_list, name='ensemble'))

    return_dict = {}
    if stat_type in ('high_res', 'all') or not stat_type:
        # extract the high res ensemble & time
        try:
            return_dict['high_res'] = merged_ds.sel(ensemble=52).dropna('time')
        except:
            pass

    if stat_type != 'high_res' or not stat_type:
        # analyze data to get statistic bands
        merged_ds = merged_ds.dropna('time')

        if stat_type == 'mean' or 'std' in stat_type or not stat_type:
            return_dict['mean'] = merged_ds.mean(dim='ensemble')
            std_ar = merged_ds.std(dim='ensemble')
            if stat_type == 'std_dev_range_upper' or not stat_type:
                return_dict['std_dev_range_upper'] = return_dict['mean'] + std_ar
            if stat_type == 'std_dev_range_lower' or not stat_type:
                return_dict['std_dev_range_lower'] = return_dict['mean'] - std_ar
        if stat_type == "min" or not stat_type:
            return_dict['min'] = merged_ds.min(dim='ensemble')
        if stat_type == "max" or not stat_type:
            return_dict['max'] = merged_ds.max(dim='ensemble')

    for key in list(return_dict):
        if units == 'english':
            # convert m3/s to ft3/s
            return_dict[key] *= M3_TO_FT3
        # convert to pandas series
        return_dict[key] = return_dict[key].to_dataframe().Qout

    return return_dict, region, reach_id, units


def get_ecmwf_ensemble(request):
    """
    Returns the statistics for the 52 member forecast
    """
    reach_id = request.args.get('reach_id', False)
    region = request.args.get('region', '')
    ensemble_number = request.args.get('ensemble', 'all')
    lat = request.args.get('lat', '')
    lon = request.args.get('lon', '')
    forecast_folder = request.args.get('date', 'most_recent')
    units = request.args.get('units', 'metric')

    if reach_id:
        region = reach_to_region(reach_id)
        if not region:
            return {"error": "Unable to determine a region paired with this reach_id"}
    elif lat != '' and lon != '' and region != '':
        point = Point(float(lat), float(lon))
        df = pd.read_csv(
            f"/app/GSP_API/region_coordinate_files/{region}/comid_lat_lon_z.csv",
            sep=',',
            header=0,
            index_col=0
        )

        points_df = df.loc[:, "Lat":"Lon"].apply(Point, axis=1)
        multi_pt = MultiPoint(points_df.tolist())

        nearest_pt = nearest_points(point, multi_pt)
        reach_id = int(points_df[points_df == nearest_pt[1]].index[0])

        if nearest_pt[0].distance(nearest_pt[1]) > 0.11:
            return {"error": "Nearest river is more than ~10km away."}
    else:
        return {"error": "Invalid reach_id or lat/lon/region combination"}, 422

    # find/check current output datasets
    path_to_output_files = os.path.join(PATH_TO_FORECASTS, region)
    forecast_nc_list, start_date = ecmwf_find_most_current_files(path_to_output_files, forecast_folder)
    if not forecast_nc_list or not start_date:
        return {"error": 'No ECMWF forecast for ' + region}, 422

    # combine 52 ensembles
    qout_datasets = []
    ensemble_index_list = []
    for forecast_nc in forecast_nc_list:
        ensemble_index_list.append(
            int(os.path.basename(forecast_nc)[:-3].split("_")[-1])
        )
        qout_datasets.append(
            xarray.open_dataset(forecast_nc)
                .sel(rivid=reach_id).Qout
        )

    merged_ds = xarray.concat(qout_datasets, pd.Index(ensemble_index_list, name='ensemble'))

    return_dict = {}
    if ensemble_number == 'all' or ensemble_number == '':
        # extract the ensembles & time
        try:
            for i in range(1, 53):
                ens = '{0:02}'.format(i)
                return_dict['ensemble_{0}'.format(ens)] = merged_ds.sel(ensemble=i).dropna('time')
        except:
            pass
    elif '-' in ensemble_number:
        # extract the ensembles & time
        if ensemble_number.split('-')[0] == '':
            start = 1
        else:
            start = int(ensemble_number.split('-')[0])

        if ensemble_number.split('-')[1] == '':
            stop = 53
        else:
            stop = int(ensemble_number.split('-')[1]) + 1

        if start > 53:
            start = 1
        if stop > 53:
            stop = 53

        try:
            for i in range(start, stop):
                ens = '{0:02}'.format(i)
                return_dict['ensemble_{0}'.format(ens)] = merged_ds.sel(ensemble=i).dropna('time')
        except:
            pass
    elif ',' in ensemble_number:
        # extract the ensembles & time
        ens_list = list(map(int, ensemble_number.replace(' ', '').split(',')))
        try:
            for i in ens_list:
                ens = '{0:02}'.format(i)
                return_dict['ensemble_{0}'.format(ens)] = merged_ds.sel(ensemble=int(i)).dropna('time')
        except:
            pass
    else:
        # extract the ensemble & time
        try:
            ens = '{0:02}'.format(int(ensemble_number))
            return_dict['ensemble_{0}'.format(ens)] = merged_ds.sel(ensemble=int(ens)).dropna('time')
        except:
            pass

    for key in list(return_dict):
        if units == 'english':
            # convert m3/s to ft3/s
            return_dict[key] *= M3_TO_FT3
        # convert to pandas series
        return_dict[key] = return_dict[key].to_dataframe().Qout

    return return_dict, region, reach_id, units


def get_historic_streamflow_series(request):
    """
    Retireve Pandas series object based on request for ERA Interim data
    """
    reach_id = request.args.get('reach_id', False)
    region = request.args.get('region', '')
    lat = request.args.get('lat', '')
    lon = request.args.get('lon', '')
    daily = request.args.get('daily', '')
    units = request.args.get('units', 'metric')

    if reach_id:
        region = reach_to_region(reach_id)
        if not region:
            return {"error": "Unable to determine a region paired with this reach_id"}
    elif lat != '' and lon != '' and region != '':
        point = Point(float(lat), float(lon))
        df = pd.read_csv(
            f"/app/GSP_API/region_coordinate_files/{region}/comid_lat_lon_z.csv",
            sep=',',
            header=0,
            index_col=0
        )

        points_df = df.loc[:, "Lat":"Lon"].apply(Point, axis=1)
        multi_pt = MultiPoint(points_df.tolist())

        nearest_pt = nearest_points(point, multi_pt)
        reach_id = int(points_df[points_df == nearest_pt[1]].index[0])

        if nearest_pt[0].distance(nearest_pt[1]) > 0.11:
            return {"error": "Nearest river is more than ~10km away."}
    else:
        return {"error": "Invalid reach_id or lat/lon/region combination"}, 422

    historical_data_file = glob(os.path.join(PATH_TO_HISTORICAL, region, 'Qout*.nc'))[0]

    # write data to csv stream
    with xarray.open_dataset(historical_data_file) as qout_nc:
        qout_data = qout_nc.sel(rivid=reach_id).Qout.to_dataframe().Qout
        if daily.lower() == 'true':
            # calculate daily values
            qout_data = qout_data.resample('D').mean()

        if units == 'english':
            # convert from m3/s to ft3/s
            qout_data *= M3_TO_FT3
    return qout_data, region, reach_id, units


def get_seasonal_average(request):
    """
    Retrieve Pandas series object based on request for seasonal average
    """
    reach_id = request.args.get('reach_id', False)
    region = request.args.get('region', '')
    lat = request.args.get('lat', '')
    lon = request.args.get('lon', '')
    units = request.args.get('units', 'metric')

    if reach_id:
        region = reach_to_region(reach_id)
        if not region:
            return {"error": "Unable to determine a region paired with this reach_id"}
    elif lat != '' and lon != '' and region != '':
        point = Point(float(lat), float(lon))
        df = pd.read_csv(
            f"/app/GSP_API/region_coordinate_files/{region}/comid_lat_lon_z.csv",
            sep=',',
            header=0,
            index_col=0
        )

        points_df = df.loc[:, "Lat":"Lon"].apply(Point, axis=1)
        multi_pt = MultiPoint(points_df.tolist())

        nearest_pt = nearest_points(point, multi_pt)
        reach_id = int(points_df[points_df == nearest_pt[1]].index[0])

        if nearest_pt[0].distance(nearest_pt[1]) > 0.11:
            return {"error": "Nearest river is more than ~10km away."}
    else:
        return {"error": "Invalid reach_id or lat/lon/region combination"}, 422

    seasonal_data_file = glob(os.path.join(PATH_TO_HISTORICAL, region, 'seasonal_average*.nc'))[0]

    # write data to csv stream
    with xarray.open_dataset(seasonal_data_file) as qout_nc:
        qout_data = qout_nc.sel(rivid=reach_id).average_flow.to_dataframe().average_flow
        if units == 'english':
            # convert from m3/s to ft3/s
            qout_data *= M3_TO_FT3
    return qout_data, region, reach_id, units


def get_return_period_dict(request):
    """
    Returns return period data as dictionary for a river ID in a watershed
    """
    reach_id = request.args.get('reach_id', False)
    region = request.args.get('region', '')
    lat = request.args.get('lat', '')
    lon = request.args.get('lon', '')
    units = request.args.get('units', 'metric')

    if reach_id:
        region = reach_to_region(reach_id)
        if not region:
            return {"error": "Unable to determine a region paired with this reach_id"}
    elif lat != '' and lon != '' and region != '':
        point = Point(float(lat), float(lon))
        df = pd.read_csv(
            f"/app/GSP_API/region_coordinate_files/{region}/comid_lat_lon_z.csv",
            sep=',',
            header=0,
            index_col=0
        )

        points_df = df.loc[:, "Lat":"Lon"].apply(Point, axis=1)
        multi_pt = MultiPoint(points_df.tolist())

        nearest_pt = nearest_points(point, multi_pt)
        reach_id = int(points_df[points_df == nearest_pt[1]].index[0])

        if nearest_pt[0].distance(nearest_pt[1]) > 0.11:
            return {"error": "Nearest river is more than ~10km away."}
    else:
        return {"error": "Invalid reach_id or lat/lon/region combination"}, 422

    return_period_file = glob(os.path.join(PATH_TO_HISTORICAL, region, 'return_period*.nc'))[0]

    # get information from dataset
    return_period_data = {}

    with xarray.open_dataset(return_period_file) as return_period_nc:
        rpd = return_period_nc.sel(rivid=reach_id)
        if units == 'english':
            rpd['max_flow'] *= M3_TO_FT3
            rpd['return_period_20'] *= M3_TO_FT3
            rpd['return_period_10'] *= M3_TO_FT3
            rpd['return_period_2'] *= M3_TO_FT3

        return_period_data["max"] = float(rpd.max_flow.values)
        return_period_data["twenty"] = float(rpd.return_period_20.values)
        return_period_data["ten"] = float(rpd.return_period_10.values)
        return_period_data["two"] = float(rpd.return_period_2.values)

    return return_period_data, region, reach_id, units
