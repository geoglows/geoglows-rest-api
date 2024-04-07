import pandas as pd
from flask import jsonify

from .data import latlon_to_reach
from .response_formatters import df_to_csv_flask_response

__all__ = [
    'get_reach_id',
]


def get_reach_id(lat: float, lon: float, *, return_format: str = 'csv'):
    """
    Finds the reach ID nearest to a given lat/lon
    Uses the ModelMasterTable to find the locations
    """
    # todo use pygeoglows
    reach_id, dist_error = latlon_to_reach(lat, lon)
    if return_format == 'csv':
        return df_to_csv_flask_response(pd.DataFrame(dict(reach_id=[reach_id, ], dist_error=[dist_error, ])),
                                        csv_name='reach_id', index=False)
    elif return_format == 'json':
        return jsonify(dict(reach_id=reach_id, dist_error=dist_error))
