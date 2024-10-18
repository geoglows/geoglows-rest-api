import os

PATH_TO_FORECASTS = "/mnt/output/v2/forecasts"
PATH_TO_FORECAST_RECORDS = "/mnt/output/v2/forecast-records"
PATH_TO_RETURN_PERIODS = "/app/return-periods.zarr"
PACKAGE_METADATA_TABLE_PATH = os.getenv(
    "PYGEOGLOWS_METADATA_TABLE_PATH", "/app/package-metadata-table.parquet"
)
NUM_DECIMALS = 1
