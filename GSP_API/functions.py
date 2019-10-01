import datetime
import os
import re
from collections import OrderedDict
from glob import glob

from pytz import utc

# GLOBAL
M3_TO_FT3 = 35.3146667


def ecmwf_find_most_current_files(path_to_watershed_files, forecast_folder):
    """""
    Finds the current output from downscaled ECMWF forecasts
    """""
    if forecast_folder == "most_recent":
        if not os.path.exists(path_to_watershed_files):
            return None, None
        directories = \
            sorted(
                [d for d in os.listdir(path_to_watershed_files)
                 if os.path.isdir(os.path.join(path_to_watershed_files, d))],
                reverse=True
            )
    else:
        directories = [forecast_folder]
    for directory in directories:
        try:
            date = datetime.datetime.strptime(directory.split(".")[0],
                                              "%Y%m%d")
            time = directory.split(".")[-1]
            path_to_files = os.path.join(path_to_watershed_files, directory)
            if os.path.exists(path_to_files):
                basin_files = sorted(glob(os.path.join(path_to_files, "*.nc")),
                                     reverse=True)
                if len(basin_files) > 0:
                    seconds = int(int(time)/100) * 60 * 60
                    forecast_datetime_utc = \
                        (date + datetime.timedelta(0, seconds))\
                        .replace(tzinfo=utc)
                    return basin_files, forecast_datetime_utc
        except Exception as ex:
            print(ex)
            pass

    # there are no files found
    return None, None


def get_ecmwf_valid_forecast_folder_list(main_watershed_forecast_folder,
                                         file_extension):
    """
    Retreives a list of valid forecast forlders for the watershed
    """
    directories = \
        sorted(
            [d for d in os.listdir(main_watershed_forecast_folder)
             if os.path.isdir(
                os.path.join(main_watershed_forecast_folder, d))],
            reverse=True
        )
    output_directories = []
    directory_count = 0
    for directory in directories:
        date = datetime.datetime.strptime(directory.split(".")[0], "%Y%m%d")
        hour = int(directory.split(".")[-1])/100
        path_to_files = os.path.join(main_watershed_forecast_folder, directory)
        if os.path.exists(path_to_files):
            basin_files = glob(os.path.join(path_to_files,
                                            "*{0}".format(file_extension)))
            # only add directory to the list if valid
            if len(basin_files) > 0:
                output_directories.append({
                    'id': directory,
                    'text': str(date + datetime.timedelta(hours=int(hour)))
                })
                directory_count += 1
            # limit number of directories
            if directory_count > 64:
                break
    return output_directories


def format_name(string):
    """
    Formats watershed name for code
    """
    if string:
        formatted_string = string.strip().replace(" ", "_").lower()
        formatted_string = re.sub(r'[^a-zA-Z0-9_-]', '', formatted_string)
        while formatted_string.startswith("-") \
                or formatted_string.startswith("_"):
            formatted_string = formatted_string[1:]
    else:
        formatted_string = ""
    return formatted_string


def format_watershed_title(watershed, subbasin):
    """
    Formats title for watershed in navigation
    """
    max_length = 30
    watershed = watershed.strip()
    subbasin = subbasin.strip()
    watershed_length = len(watershed)
    if watershed_length > max_length:
        return watershed[:max_length-1].strip() + "..."
    max_length -= watershed_length
    subbasin_length = len(subbasin)
    if subbasin_length > max_length:
        return watershed + " (" + subbasin[:max_length-3].strip() + " ...)"
    return watershed + " (" + subbasin + ")"


def get_units_title(unit_type):
    """
    Get the title for units
    """
    units_title = "m"
    if unit_type == 'english':
        units_title = "ft"
    return units_title


def reach_to_region(reach_id=None):
    # Indonesia 1M's
    # ------australia 2M (currently 200k's)
    # Japan 3M's
    # East Asia 4M's
    # South Asia 5M's
    # ------middle_east 6M (currently 600k's)
    # Africa 7M's
    # Central Asia 8M's
    # South America 9M's
    # West Asia 10M's
    # -------central_america 11M (currently 900k's)
    # Europe 12M's
    # North America 13M's

    lookup = OrderedDict([
        ('south_asia-mainland', 100000),
        ('error', 200000),
        ('australia-geoglows', 300000),
        ('middle_east-geoglows', 700000),
        ('central_america-geoglows', 1000000),
        # ('indonesia-geoglows', 2000000),
        # ('japan-geoglows', 4000000),
        # ('east_asia-geoglows', 5000000),
        ('south_asia-geoglows', 6000000),
        ('africa-geoglows', 8000000),
        # ('central_asia-geoglows', 9000000),
        ('south_america-geoglows', 10000000),
        # ('west_asia-geoglows', 11000000),
        # ('europe-geoglows', 13000000),
        # ('north_america-geoglows', 14000000)
    ])
    for region, threshold in lookup.items():
        if reach_id < threshold:
            if region == 'error':
                return False
            return region
    return 'error'
