import xarray as xr
from datetime import datetime


class DataArraysHelper:
    @staticmethod
    def time_range(data_arrays: list[xr.DataArray]) -> tuple[datetime, datetime]:
        start_time = min(data_array.attrs["start_time"] for data_array in data_arrays)
        end_time = max(data_array.attrs["end_time"] for data_array in data_arrays)
        return start_time, end_time
