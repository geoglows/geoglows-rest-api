import os
from glob import glob
import pandas as pd
import xarray

from csv import writer as csv_writer
from flask import make_response
from io import StringIO

from functions import get_units_title

from shapely.geometry import Point, MultiPoint
from shapely.ops import nearest_points

from functions import (ecmwf_find_most_current_files,
                       M3_TO_FT3)


def get_forecast_streamflow_csv(request):
    """
    Retrieve the forecasted streamflow as CSV
    """

    try:
        # retrieve statistics
        forecast_statistics, watershed_name, subbasin_name, river_id, units = \
            get_ecmwf_forecast_statistics(request)
        
        # prepare to write response for CSV
        si = StringIO()

        writer = csv_writer(si)
        forecast_df = pd.DataFrame(forecast_statistics)
        column_names = (forecast_df.columns.values +
                        [' ({}3/s)'.format(get_units_title(units))]
                        ).tolist()

        writer.writerow(['datetime'] + column_names)

        for row_data in forecast_df.itertuples():
            writer.writerow(row_data)

        response = make_response(si.getvalue())
        response.headers['content-type'] = 'text/csv'
        response.headers['Content-Disposition'] = \
            'attachment; filename=forecasted_streamflow_{0}_{1}_{2}.csv' \
            .format(watershed_name,
                    subbasin_name,
                    river_id)

        return response

    except:
        return {"error": "An unexpected error occured with the CSV response."}
    

def get_forecast_ensemble_csv(request):
    """
    Retrieve the forecast ensemble as CSV
    """
    try:
        # retrieve ensembles
        forecast_statistics, watershed_name, subbasin_name, river_id, units = \
            get_ecmwf_ensemble(request)
    
        # prepare to write response for CSV
        si = StringIO()
    
        writer = csv_writer(si)
    
        forecast_df = pd.DataFrame(forecast_statistics)
        column_names = (forecast_df.columns.values +
                        [' ({}3/s)'.format(get_units_title(units))]
                        ).tolist()
    
        writer.writerow(['datetime'] + column_names)
    
        for row_data in forecast_df.itertuples():
            writer.writerow(row_data)
            
        # prepare to write response for CSV
        response = make_response(si.getvalue())
        response.headers['content-type'] = 'text/csv'
        response.headers['Content-Disposition'] = \
            'attachment; filename=forecasted_ensembles_{0}_{1}_{2}.csv' \
            .format(watershed_name,
                    subbasin_name,
                    river_id)
    
        return response
    except:
        return {"error": "An unexpected error occured with the CSV response."}
        

def get_historic_data_csv(request):
    """""
    Returns ERA Interim data as csv
    """""

    try:
        qout_data, river_id, watershed_name, subbasin_name, units =\
            get_historic_streamflow_series(request)
    
        si = StringIO()
    
        writer = csv_writer(si)
        
        writer.writerow(['datetime', 'streamflow ({}3/s)'
                             .format(get_units_title(units))])
    
        for row_data in qout_data.iteritems():
            writer.writerow(row_data)
            
        # prepare to write response
        response = make_response(si.getvalue())
        response.headers['content-type'] = 'text/csv'
        response.headers['Content-Disposition'] = \
            'attachment; filename=historic_streamflow_{0}_{1}_{2}.csv' \
            .format(watershed_name,
                    subbasin_name,
                    river_id)

        return response
    except:
        return {"error": "An unexpected error occured with the CSV response."}


def get_ecmwf_forecast_statistics(request):
    """
    Returns the statistics for the 52 member forecast
    """

    params = {"region": request.args.get('region', ''),
              "reach_id": request.args.get('reach_id', ''),
              "lat": request.args.get('lat', ''),
              "lon": request.args.get('lon', ''),
              "stat": request.args.get('stat', ''),
              "date": request.args.get('date', ''),
              "return_format": request.args.get('return_format', '')}

    path_to_rapid_output = "/mnt/output/ecmwf"

    watershed_name = params["region"].split("-")[0]
    subbasin_name = params["region"].split("-")[1]
    
    if params["reach_id"] != '':
        river_id = int(params["reach_id"])
    elif params["lat"] != '' and params["lon"] != '':
        point = Point(float(params["lat"]),float(params["lon"]))
        df = pd.read_csv(
            f"/app/GSP_API/region_coordinate_files/{params['region']}/comid_lat_lon_z.csv", 
            sep=',',
            header=0,
            index_col=0
        )
        
        points_df = df.loc[:,"Lat":"Lon"].apply(Point,axis=1)
        multi_pt = MultiPoint(points_df.tolist())
        
        nearest_pt = nearest_points(point, multi_pt)
        river_id = int(points_df[points_df == nearest_pt[1]].index[0])

        if nearest_pt[0].distance(nearest_pt[1]) > 0.11:
            return {"error": "Nearest river is more than ~10km away."}
    else:
        return {"error": "No river_id or coordinates found in parameters."}
    
    units = "metric"

    if params["date"] == '':
        forecast_folder = 'most_recent'
    else:
        forecast_folder = params["date"]

    stat_type = params["stat"]
    if (stat_type is None or stat_type == "all"):
        stat_type = ""

    # find/check current output datasets
    path_to_output_files = \
        os.path.join(path_to_rapid_output,
                     "{0}-{1}".format(watershed_name, subbasin_name))
    forecast_nc_list, start_date = \
        ecmwf_find_most_current_files(path_to_output_files, forecast_folder)
    if (not forecast_nc_list or not start_date):
        return {"error": 'ECMWF forecast for %s (%s).' % (watershed_name, subbasin_name)}

    # combine 52 ensembles
    qout_datasets = []
    ensemble_index_list = []
    for forecast_nc in forecast_nc_list:
        ensemble_index_list.append(
            int(os.path.basename(forecast_nc)[:-3].split("_")[-1])
        )
        qout_datasets.append(
            xarray.open_dataset(forecast_nc)
                  .sel(rivid=river_id).Qout
        )

    merged_ds = xarray.concat(qout_datasets,
                              pd.Index(ensemble_index_list, name='ensemble'))

    return_dict = {}
    if (stat_type == 'high_res' or not stat_type):
        # extract the high res ensemble & time
        try:
            return_dict['high_res'] = merged_ds.sel(ensemble=52).dropna('time')
        except:
            pass

    if (stat_type != 'high_res' or not stat_type):
        # analyze data to get statistic bands
        merged_ds = merged_ds.dropna('time')

        if (stat_type == 'mean' or 'std' in stat_type or not stat_type):
            return_dict['mean'] = merged_ds.mean(dim='ensemble')
            std_ar = merged_ds.std(dim='ensemble')
            if (stat_type == 'std_dev_range_upper' or not stat_type):
                return_dict['std_dev_range_upper'] = \
                    return_dict['mean'] + std_ar
            if (stat_type == 'std_dev_range_lower' or not stat_type):
                return_dict['std_dev_range_lower'] = \
                    return_dict['mean'] - std_ar
        if (stat_type == "min" or not stat_type):
            return_dict['min'] = merged_ds.min(dim='ensemble')
        if (stat_type == "max" or not stat_type):
            return_dict['max'] = merged_ds.max(dim='ensemble')

    for key in list(return_dict):
        if (units == 'english'):
            # convert m3/s to ft3/s
            return_dict[key] *= M3_TO_FT3
        # convert to pandas series
        return_dict[key] = return_dict[key].to_dataframe().Qout

    return return_dict, watershed_name, subbasin_name, river_id, units


def get_ecmwf_ensemble(request):
    """
    Returns the statistics for the 52 member forecast
    """
    params = {"region": request.args.get('region', ''),
              "reach_id": request.args.get('reach_id', ''),
              "ensemble": request.args.get('ensemble', ''),
              "lat": request.args.get('lat', ''),
              "lon": request.args.get('lon', ''),
              "date": request.args.get('date', ''),
              "return_format": request.args.get('return_format', '')}

    path_to_rapid_output = "/mnt/output/ecmwf"

    watershed_name = params["region"].split("-")[0]
    subbasin_name = params["region"].split("-")[1]

    if params["reach_id"] != '':
        river_id = int(params["reach_id"])
    elif params["lat"] != '' and params["lon"] != '':
        point = Point(float(params["lat"]),float(params["lon"]))
        df = pd.read_csv(
            f"/app/GSP_API/region_coordinate_files/{params['region']}/comid_lat_lon_z.csv", 
            sep=',',
            header=0,
            index_col=0
        )
        
        points_df = df.loc[:,"Lat":"Lon"].apply(Point,axis=1)
        multi_pt = MultiPoint(points_df.tolist())
        
        nearest_pt = nearest_points(point, multi_pt)
        river_id = int(points_df[points_df == nearest_pt[1]].index[0])

        if nearest_pt[0].distance(nearest_pt[1]) > 0.11:
            return {"error": "Nearest river is more than ~10km away."}
    else:
        return {"error": "No river_id or coordinates found in parameters."}

    units = "metric"

    if params["date"] == '':
        forecast_folder = 'most_recent'
    else:
        forecast_folder = params["date"]

    ensemble_number = params['ensemble']
    if ensemble_number is None:
        ensemble_number = "all"

    # find/check current output datasets
    path_to_output_files = \
        os.path.join(path_to_rapid_output,
                     "{0}-{1}".format(watershed_name, subbasin_name))
    forecast_nc_list, start_date = \
        ecmwf_find_most_current_files(path_to_output_files, forecast_folder)
    if not forecast_nc_list or not start_date:
        return {"error": 'ECMWF forecast for %s (%s).'
                            % (watershed_name, subbasin_name)}

    # combine 52 ensembles
    qout_datasets = []
    ensemble_index_list = []
    for forecast_nc in forecast_nc_list:
        ensemble_index_list.append(
            int(os.path.basename(forecast_nc)[:-3].split("_")[-1])
        )
        qout_datasets.append(
            xarray.open_dataset(forecast_nc)
                  .sel(rivid=river_id).Qout
        )

    merged_ds = xarray.concat(qout_datasets,
                              pd.Index(ensemble_index_list, name='ensemble'))

    return_dict = {}
    if (ensemble_number == 'all' or ensemble_number == ''):
        # extract the ensembles & time
        try:
            for i in range(1,53):
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
            stop = int(ensemble_number.split('-')[1])+1

        if start > 53:
            start = 1
        if stop > 53:
            stop = 53

        try:
            for i in range(start,stop):
                ens = '{0:02}'.format(i)
                return_dict['ensemble_{0}'.format(ens)] = merged_ds.sel(ensemble=i).dropna('time')
        except:
            pass
    elif ',' in ensemble_number:
        # extract the ensembles & time
        ens_list = list(map(int,ensemble_number.replace(' ', '').split(',')))
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

    return return_dict, watershed_name, subbasin_name, river_id, units


def get_historic_streamflow_series(request):
    """
    Retireve Pandas series object based on request for ERA Interim data
    """
    
    params = {"region": request.args.get('region', ''),
              "reach_id": request.args.get('reach_id', ''),
              "lat": request.args.get('lat', ''),
              "lon": request.args.get('lon', ''),
              "daily": request.args.get('daily', '')}

    path_to_rapid_output = "/mnt/output/era"
    
    # get information from GET request
    daily = request.args.get('daily', '')
    units = 'metric'
    historical_data_file = glob(os.path.join(path_to_rapid_output, params["region"], 'Qout*.nc'))[0]
    
    if params["reach_id"] != '':
        river_id = int(params["reach_id"])
    elif params["lat"] != '' and params["lon"] != '':
        point = Point(float(params["lat"]),float(params["lon"]))
        df = pd.read_csv(
            f"/app/GSP_API/region_coordinate_files/{params['region']}/comid_lat_lon_z.csv", 
            sep=',',
            header=0,
            index_col=0
        )
        
        points_df = df.loc[:,"Lat":"Lon"].apply(Point,axis=1)
        multi_pt = MultiPoint(points_df.tolist())
        
        nearest_pt = nearest_points(point, multi_pt)
        river_id = int(points_df[points_df == nearest_pt[1]].index[0])

        if nearest_pt[0].distance(nearest_pt[1]) > 0.11:
            return {"error": "Nearest river is more than ~10km away."}
    else:
        return {"error": "No river_id or coordinates found in parameters."}

    # write data to csv stream
    with xarray.open_dataset(historical_data_file) as qout_nc:
        qout_data = qout_nc.sel(rivid=river_id).Qout\
                           .to_dataframe().Qout
        if daily.lower() == 'true':
            # calculate daily values
            qout_data = qout_data.resample('D').mean()

        if units == 'english':
            # convert from m3/s to ft3/s
            qout_data *= M3_TO_FT3
    return qout_data, river_id, params["region"].split('-')[0], params["region"].split('-')[0], units
