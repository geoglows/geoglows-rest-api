import datetime
import pandas as pd

from flask import render_template
from model_utilities import reach_to_region

from v2_controllers_forecasts import forecast_stats
from v2_controllers_historical import historical


def get_sites() -> render_template:
    a = pd.read_csv('/app/GSP_API/geometry/region_coordinate_files/global_coordinate_file.csv', index_col=0)
    a['COMID'] = a['COMID'].astype(int)
    return render_template('water_one_flow/GetSites_template.xml',
                           creation_time=datetime.datetime.utcnow().strftime('%Y-%m-%dT%XZ'),
                           comid_lat_lon_array=a.to_numpy(), )


def get_variables():
    return "variables list"


def get_values(location: int, variable: str, start_date: str, end_date: str) -> render_template:
    if location is None:
        raise ValueError('Must provide a location with ?location=reach_id')
    else:
        location = int(location)

    variable = variable.lower()

    units = 'metric'
    region = reach_to_region(location)
    generation_date = datetime.datetime.now().strftime("%Y-%m-%d %X")

    if variable in ('forecaststats', 'forecast_stats', 'forecast', 'stats',):
        # 3000150
        df = forecast_stats(location, 'most_recent', units, 'df')
        start_date = df.index[0]
        end_date = df.index[-1]
        stats_data_array = [(column_name, list(zip(*[df.index, df[column_name]]))) for column_name in df.columns]
        return render_template('water_one_flow/GetValues_template_ForecastStats.xml',
                               reach_id=location,
                               generation_date=generation_date,
                               start_date=start_date,
                               end_date=end_date,
                               units=units,
                               stats_data_array=stats_data_array )

    elif variable in ('historicsimulation', 'historic_simulation', 'historical', 'historic', 'historical_simulation'):
        # 5084091
        forcing = 'era_5'
        df = historical(location, units, 'df')
        start_date = df.index[0]
        end_date = df.index[-1]
        return render_template('water_one_flow/GetValues_template_HistoricSimulation.xml',
                               reach_id=location,
                               generation_date=generation_date,
                               start_date=start_date,
                               end_date=end_date,
                               units=units,
                               time_value_array=list(zip(*[df.index, df.to_numpy()])), )
