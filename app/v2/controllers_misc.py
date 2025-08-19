from flask import jsonify
from .constants import PYGEOGLOWS_EXTRA_METADATA_TABLE_PATH
import pandas as pd

__all__ = [
    'get_river_id',
]

def get_river_id(lat: float, lon: float):
    """
    Finds the river ID nearest to a given lat/lon
    Uses the ModelMasterTable to find the locations
    """
    df = pd.read_parquet(PYGEOGLOWS_EXTRA_METADATA_TABLE_PATH, columns=['LINKNO', 'lat', 'lon'])
    df['dist'] = ((df['lat'] - float(lat)) ** 2 + (df['lon'] - float(lon)) ** 2) ** 0.5
    river_id = df.loc[lambda x: x['dist'] == df['dist'].min(), 'LINKNO'].values[0]
    return jsonify(dict(river_id=int(river_id)))
