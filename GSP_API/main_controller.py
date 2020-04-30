import os
from csv import writer as csv_writer
from glob import glob
from io import StringIO
from collections import namedtuple
import math

import pandas as pd
import xarray
from flask import make_response
from functions import ecmwf_find_most_current_files, M3_TO_FT3, get_units_title, reach_to_region
from shapely.geometry import Point, MultiPoint, box
from shapely.ops import nearest_points

# GLOBAL
PATH_TO_ERA_INTERIM = '/mnt/output/era-interim'
PATH_TO_ERA_5 = '/mnt/output/era-5'
PATH_TO_FORECASTS = '/mnt/output/forecasts'
PATH_TO_FORECAST_RECORDS = '/mnt/output/forecast-records'


def get_forecast_streamflow_csv(request):
    """
    Retrieve the forecasted streamflow as CSV
    """

    try:
        # retrieve statistics
        forecast_statistics, region, reach_id, units = get_ecmwf_forecast_statistics(request)

        # prepare to write response for CSV
        si = StringIO()
        writer = csv_writer(si)

        forecast_df = pd.DataFrame(forecast_statistics)
        column_names = (forecast_df.columns.values + [' ({}3/s)'.format(get_units_title(units)[0])]).tolist()
        writer.writerow(['datetime'] + column_names)

        for row_data in forecast_df.itertuples():
            writer.writerow(row_data)

        response = make_response(si.getvalue())
        response.headers['content-type'] = 'text/csv'
        response.headers['Content-Disposition'] = \
            'attachment; filename=forecasted_streamflow_{0}_{1}.csv'.format(region, reach_id)

        return response

    except:
        return {"error": "An unexpected error occurred with the CSV response."}, 422


def get_forecast_ensemble_csv(request):
    """
    Retrieve the forecast ensemble as CSV
    """
    try:
        # retrieve ensembles
        forecast_statistics, region, reach_id, units = get_ecmwf_ensemble(request)

        # prepare to write response for CSV
        si = StringIO()
        writer = csv_writer(si)

        forecast_df = pd.DataFrame(forecast_statistics)
        column_names = (forecast_df.columns.values + [' ({}3/s)'.format(get_units_title(units)[0])]).tolist()

        writer.writerow(['datetime'] + column_names)

        for row_data in forecast_df.itertuples():
            writer.writerow(row_data)

        # prepare to write response for CSV
        response = make_response(si.getvalue())
        response.headers['content-type'] = 'text/csv'
        response.headers['Content-Disposition'] = \
            'attachment; filename=forecasted_ensembles_{0}_{1}.csv'.format(region, reach_id)

        return response
    except:
        return {"error": "An unexpected error occurred with the CSV response."}, 422


def get_ecmwf_forecast_statistics(request):
    """
    Returns the statistics for the 52 member forecast
    """
    reach_id = int(request.args.get('reach_id', False))
    stat_type = request.args.get('stat', '')
    lat = request.args.get('lat', '')
    lon = request.args.get('lon', '')
    forecast_folder = request.args.get('date', 'most_recent')
    units = request.args.get('units', 'metric')

    if reach_id:
        region = reach_to_region(reach_id)
        if not region:
            return {"error": "Unable to determine a region paired with this reach_id"}
    elif lat != '' and lon != '':
        reach_id, region, dist_error = get_reach_from_latlon(lat, lon)
        if dist_error:
            return dist_error
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
    reach_id = int(request.args.get('reach_id', False))
    ensemble_number = request.args.get('ensemble', 'all')
    lat = request.args.get('lat', '')
    lon = request.args.get('lon', '')
    forecast_folder = request.args.get('date', 'most_recent')
    units = request.args.get('units', 'metric')

    if reach_id:
        region = reach_to_region(reach_id)
        if not region:
            return {"error": "Unable to determine a region paired with this reach_id"}
    elif lat != '' and lon != '':
        reach_id, region, dist_error = get_reach_from_latlon(lat, lon)
        if dist_error:
            return dist_error
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


def get_reach_from_latlon(lat, lon):
    # create a shapely point for the querying
    point = Point(float(lon), float(lat))
    regions_to_check = []
    # store the best matching stream using a named tuple for easy comparisons/management
    StreamResult = namedtuple('Stream', 'reach_id, region, distance')
    stream_result = StreamResult(None, None, math.inf)

    # open the bounding boxes csv, figure out which regions the point lies within
    bb_csv = pd.read_csv('/app/GSP_API/region_coordinate_files/bounding_boxes.csv', index_col='region')
    for row in bb_csv.iterrows():
        bbox = box(row[1][0], row[1][1], row[1][2], row[1][3])
        if point.within(bbox):
            regions_to_check.append(row[0])

    # if there weren't any regions, return that there was an error
    if len(regions_to_check) == 0:
        return {"error": "This point is not within any of the supported delineation regions."}

    # switch the point because the csv's are lat/lon, backwards from what shapely expects (lon then lat)
    point = Point(float(lat), float(lon))

    # check the lat lon against each of the region csv's that we determined were an option
    for region in regions_to_check:
        # open the region csv, find the closest reach_id
        df = pd.read_csv(
            f"/app/GSP_API/region_coordinate_files/{region}/comid_lat_lon_z.csv", sep=',', header=0, index_col=0)
        points_df = df.loc[:, "Lat":"Lon"].apply(Point, axis=1)
        multi_pt = MultiPoint(points_df.tolist())
        nearest_pt = nearest_points(point, multi_pt)
        reach_id = int(points_df[points_df == nearest_pt[1]].index[0])

        # is this a better match than what we have? if so then replace the current selection
        distance = nearest_pt[0].distance(nearest_pt[1])
        if distance < stream_result.distance:
            stream_result = StreamResult(reach_id, region, distance)

    # if the stream was too far away, set the error message
    if stream_result.distance > 0.11:
        distance_error = {"error": "Nearest river is more than ~10km away."}
    else:
        distance_error = False

    return stream_result.reach_id, stream_result.region, distance_error


def get_region_from_latlon(lat, lon):
    # create a shapely point for the querying
    point = Point(float(lon), float(lat))

    # open the bounding boxes csv, figure out which regions the point lies within
    bb_csv = pd.read_csv('/app/GSP_API/region_coordinate_files/bounding_boxes.csv', index_col='region')
    for row in bb_csv.iterrows():
        bbox = box(row[1][0], row[1][1], row[1][2], row[1][3])
        if point.within(bbox):
            return row[0]

    raise ValueError('given lat and lon not found within the bounding boxes of a forecast delineation')


def get_forecast_warnings(region, lat, lon, forecast_date='most_recent'):
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
            return {"error": 'Forecast date {0} was not found'.format(forecast_date)}

    # locate the forecast warning csv
    summary_file = os.path.join(folder, 'forecasted_return_periods_summary.csv')

    if not os.path.isfile(summary_file):
        return {"error": "summary file was not found for this region and forecast date"}
    warning_summary = pd.read_csv(summary_file)

    # prepare to write response for CSV
    si = StringIO()
    writer = csv_writer(si)

    writer.writerow([''] + warning_summary.columns.values.tolist())

    for row_data in warning_summary.itertuples():
        writer.writerow(row_data)
    response = make_response(si.getvalue())
    response.headers['content-type'] = 'text/csv'
    response.headers['Content-Disposition'] = 'attachment; filename=ForecastWarnings-{0}.csv'.format(region)

    return response
