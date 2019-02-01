import os
import pandas as pd
import xarray

from csv import writer as csv_writer
from flask import make_response
from io import StringIO

from functions import get_units_title, shortest_dist

import logging


# create logger function
def init_logger():
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    handler = logging.FileHandler('/app/api.log', 'a')
    formatter = logging.Formatter('%(asctime)s: %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)


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
        return "An unexpected error occured with the CSV response"


def get_ecmwf_forecast_statistics(request):
    """
    Returns the statistics for the 52 member forecast
    """
    
    init_logger()
    
    params = {"region": request.args.get('region', ''),
              "reach_id": request.args.get('reach_id', ''),
              "lat": request.args.get('lat', ''),
              "lon": request.args.get('lon', ''),
              "stat": request.args.get('stat', ''),
              "return_format": request.args.get('return_format', '')}

    path_to_rapid_output = "/mnt/output"

    watershed_name = params["region"].split("-")[0]
    subbasin_name = params["region"].split("-")[1]
    
    river_id = int(params["reach_id"])
#    if params["reach_id"] != '':
#        river_id = int(params["reach_id"])
#    elif params["lat"] != '' and params["lon"] != '':
#        coor = [float(params["lat"]), float(params["lon"])]
#        df = pd.read_csv(
#            f'/app/GSP_API/region_coordinate_files/{params["region"]}/comid_lat_lon_z.csv', 
#            sep=',',
#            header=0,
#            index_col=0
#        )
#        
#        river_id, distance = shortest_dist(coor, df)
#
#        if distance > 0.11:
#            distance = "Nearest river is more than ~10km away."
        
    units = "metric"

    forecast_folder = 'most_recent'

    stat_type = params["stat"]
    if (stat_type is None):
        stat_type = ""

    # find/check current output datasets
    path_to_output_files = \
        os.path.join(path_to_rapid_output,
                     "{0}-{1}".format(watershed_name, subbasin_name))
    forecast_nc_list, start_date = \
        ecmwf_find_most_current_files(path_to_output_files, forecast_folder)
    if (not forecast_nc_list or not start_date):
        logging.error('ECMWF forecast for %s (%s).' % (watershed_name, subbasin_name))

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
