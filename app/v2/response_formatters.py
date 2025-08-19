import datetime

import numpy as np
import pandas as pd
from flask import make_response, jsonify

__all__ = ['df_to_csv_flask_response', 'df_to_jsonify_response', 'new_json_template', ]


def df_to_csv_flask_response(df: pd.DataFrame, csv_name: str, *, index: bool = True):
    response = make_response(df.to_csv(index=index))
    response.headers['content-type'] = 'text/csv'
    response.headers['Content-Disposition'] = f'attachment; filename={csv_name}.csv'
    return response


def df_to_jsonify_response(df: pd.DataFrame, river_id: int):
    if isinstance(df.index, pd.DatetimeIndex):
        df.index = df.index.strftime('%Y-%m-%dT%X+00:00')
    json_template = new_json_template(river_id, start_date=df.index[0], end_date=df.index[-1])
    json_template['datetime'] = df.index.tolist()
    json_template.update(df.replace(np.nan, '').to_dict(orient='list'))
    return jsonify(json_template)


def new_json_template(river_id, start_date, end_date):
    # Ensure start_date and end_date are properly formatted because of monthly_average function
    def clean_value(value):
        if isinstance(value, (np.int32, np.int64)):
            return int(value)  # Convert NumPy int to Python int
        elif isinstance(value, pd.Timestamp):
            return value.strftime('%Y-%m-%dT%X+00:00')  # Convert Pandas Timestamp to string
        elif isinstance(value, str):
            return value  # Keep strings
        else:
            return value

    return {
        'metadata': {
            'river_id': river_id,
            'gen_date': datetime.datetime.now(datetime.UTC).strftime('%Y-%m-%dT%X+00:00'),
            'start_date': clean_value(start_date),
            'end_date': clean_value(end_date),
            'series': [],
            'units': {
                'name': 'streamflow',
                'short': 'cms',
                'long': 'cubic meters per second',
            },
        }
    }
