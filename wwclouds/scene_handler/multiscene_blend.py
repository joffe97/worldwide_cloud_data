import xarray as xr
import dask.array as da
import numpy as np
from pyresample import AreaDefinition


class EqcMean:
    MAX_DEGREES_LON = 360
    MAX_DEGREES_LAT = 180

    def __init__(self, data_arrays: list[xr.DataArray]):
        first_array = data_arrays[0]
        first_area: AreaDefinition = first_array.attrs["area"]

        lons, lats = first_area.get_lonlats()

        self.lon_delta_step = abs(lons[0][1] - lons[0][0])
        self.lat_delta_step = abs(lats[1][0] - lats[0][0])

        self.lon_len = int(360 // self.lon_delta_step)
        self.lat_len = int(180 // self.lat_delta_step)

        self.data = np.empty((self.lat_len, self.lon_len))
        self.data[:] = np.nan

    def translate_coords_to_earth_array_indexes(self, coord: tuple[float, float]) -> tuple[int, int]:
        if len(coord) != 2:
            raise ValueError("coord cannot contain more than two values")
        coord = [(coord[0] + 180) % 360, (coord[1] + 90) % 180]
        earth_array_pos = (
            round(coord[0] / self.lon_delta_step) % self.lon_len,
            round(coord[1] / self.lat_delta_step) % self.lat_len
        )
        return earth_array_pos

    def as_data_array(self) -> xr.DataArray:
        data = da.from_array(self.data)
        return xr.DataArray(data, dims=["x", "y"])

    def add_value_to_coordinate(self, value: float, coordinate: tuple[float, float]):
        if np.isnan(value):
            return
        lon_index, lat_index = self.translate_coords_to_earth_array_indexes(coordinate)
        old_value = value if np.isnan(self.data[lat_index][lon_index]) else self.data[lat_index][lon_index]
        self.data[lat_index][lon_index] = (old_value + value) / 2

    def add_data_array(self, data_array: xr.DataArray) -> None:
        values = data_array.values
        area: AreaDefinition = data_array.attrs["area"]
        lons, lats = area.get_lonlats()
        longitude_list = lons[0]
        latitude_list = list(reversed(list(map(lambda val: val[0], lats))))
        for lat_index, value_row in enumerate(values):
            for lon_index, value in enumerate(value_row):
                coord = (longitude_list[lon_index], latitude_list[lat_index])
                self.add_value_to_coordinate(value, coord)

    def blend(self, data_arrays: list[xr.DataArray]) -> None:
        for data_array in data_arrays:
            self.add_data_array(data_array)

    @staticmethod
    def blend_func(data_arrays: list[xr.DataArray]) -> xr.DataArray:
        self = EqcMean(data_arrays)
        self.blend(data_arrays)
        return self.as_data_array()
