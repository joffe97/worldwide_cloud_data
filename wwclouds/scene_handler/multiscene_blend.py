import xarray as xr
import dask.array as da
import numpy as np
from pyresample import AreaDefinition
from datetime import datetime
from enum import Enum, auto


class Axis(Enum):
    LON = auto()
    LAT = auto()

    @property
    def length_in_metres(self) -> float:
        if self == Axis.LON:
            return 40_075_017.0
        elif self == Axis.LAT:
            return 20_003_931.5
        else:
            raise ValueError("earth_radius_in_metres is not implemented for the given axis")

    @property
    def max_degree(self):
        if self == Axis.LON:
            return 360
        elif self == Axis.LAT:
            return 180
        else:
            raise ValueError("max_degree is not implemented for the given axis")


class EqcMean:
    def __init__(self, data_arrays: list[xr.DataArray]):
        if len(data_arrays) == 0:
            raise ValueError("cannot create EqcMean object with 0 DataArrays")
        self.data_arrays = data_arrays
        self.lon_delta_step, self.lat_delta_step = self.__get_lonlats_delta_steps(10)

        self.data = np.empty((self.lat_len, self.lon_len))
        self.data[:] = np.nan

    @property
    def lon_len(self) -> int:
        return int(Axis.LON.max_degree // self.lon_delta_step)

    @property
    def lat_len(self) -> int:
        return int(Axis.LAT.max_degree // self.lat_delta_step)

    @property
    def first_data_array(self) -> xr.DataArray:
        return self.data_arrays[0]

    @property
    def time_range(self) -> tuple[datetime, datetime]:
        start_time = min(data_array.attrs["start_time"] for data_array in self.data_arrays)
        end_time = max(data_array.attrs["end_time"] for data_array in self.data_arrays)
        return start_time, end_time

    @property
    def lonlats(self):
        lats_sorted = self.__get_axis_sorted(Axis.LAT)
        lons = self.__get_axis_sorted(Axis.LON)
        lats = np.flip(lats_sorted)
        return lons, lats

    @property
    def coords(self):
        lons, lats = self.lonlats
        return {
            "x": lons,
            "y": lats
        }

    @property
    def area_extent(self) -> tuple[float, float, float, float]:
        return (
            -Axis.LON.length_in_metres / 2,
            -Axis.LAT.length_in_metres / 2,
            Axis.LON.length_in_metres / 2,
            Axis.LAT.length_in_metres / 2
        )

    @property
    def area_def(self) -> AreaDefinition:
        area: AreaDefinition = self.first_data_array.attrs["area"]
        projection = area.proj_dict
        projection["lon_0"] = 0.0
        args = {
            "area_id": "eqc_area_earth",
            "description": "EQC projection of the whole earth",
            "projection": projection,
            "width": self.lon_len,
            "height": self.lat_len,
            "area_extent": self.area_extent
        }
        return area.copy(**args)

    def __get_axis_sorted_in_degrees(self, axis: Axis) -> np.ndarray:
        max_value = axis.max_degree
        delta_step = getattr(self, f"{axis.name.lower()}_delta_step")
        axis_len_aim = getattr(self, f"{axis.name.lower()}_len")

        axis = np.arange(0, max_value, delta_step)
        overflow = len(axis) - axis_len_aim
        if overflow < 0:
            ValueError("axis cannot be less than the aim")
        axis.resize(axis_len_aim)
        return axis

    def __get_axis_sorted(self, axis: Axis) -> np.ndarray:
        max_value = axis.length_in_metres // 2
        axis_len_aim = getattr(self, f"{axis.name.lower()}_len")
        delta_step = (2 * max_value) // axis_len_aim

        axis = np.arange(-max_value, max_value, delta_step)
        overflow = len(axis) - axis_len_aim
        if overflow < 0:
            ValueError("axis cannot be less than the aim")
        axis.resize(axis_len_aim)
        return axis

    def __get_lonlats_delta_steps(self, max_samples=10) -> tuple[float, float]:
        lonlats_sample_lists = [[], []]
        for cur_array in self.data_arrays:
            cur_area: AreaDefinition = cur_array.attrs["area"]

            lons, lats = cur_area.get_lonlats()
            lon_gen = (lon for lon in lons[0])
            lat_gen = (lat[0] for lat in lats)

            for index, axis_gen in enumerate([lon_gen, lat_gen]):
                axis_sample_list = lonlats_sample_lists[index]
                try:
                    prev = next(axis_gen)
                    for _ in range(max_samples):
                        cur = next(axis_gen)
                        delta_step_sample = abs(cur - prev)
                        axis_sample_list.append(delta_step_sample)
                        prev = cur
                except StopIteration:
                    continue
        return tuple(map(max, lonlats_sample_lists))

    def __translate_coords_to_earth_array_indexes(self, coord: tuple[float, float]) -> tuple[int, int]:
        if len(coord) != 2:
            raise ValueError("coord cannot contain more than two values")
        coord = [
            (coord[0] + (Axis.LON.max_degree // 2)) % Axis.LON.max_degree,
            (coord[1] + (Axis.LAT.max_degree // 2)) % Axis.LAT.max_degree
        ]
        earth_array_pos = (
            round(coord[0] / self.lon_delta_step) % self.lon_len,
            round(coord[1] / self.lat_delta_step) % self.lat_len
        )
        return earth_array_pos

    def __add_value_to_coordinate(self, value: float, coordinate: tuple[float, float]):
        if np.isnan(value):
            return
        lon_index, lat_index = self.__translate_coords_to_earth_array_indexes(coordinate)
        old_value = value if np.isnan(self.data[lat_index][lon_index]) else self.data[lat_index][lon_index]
        self.data[lat_index][lon_index] = (old_value + value) / 2

    def __add_data_array(self, data_array: xr.DataArray) -> None:
        values = data_array.values
        area: AreaDefinition = data_array.attrs["area"]
        lons, lats = area.get_lonlats()
        longitude_list = lons[0]
        latitude_list = list(reversed(list(map(lambda val: val[0], lats))))
        for lat_index, value_row in enumerate(values):
            for lon_index, value in enumerate(value_row):
                coord = (longitude_list[lon_index], latitude_list[lat_index])
                self.__add_value_to_coordinate(value, coord)

    def as_data_array(self) -> xr.DataArray:
        data = da.from_array(self.data)
        start_time, end_time = self.time_range
        attrs = {
            "start_time": start_time,
            "end_time": end_time,
            "area": self.area_def
        }
        return xr.DataArray(
            data,
            dims=["y", "x"],
            coords=self.coords,
            attrs=attrs
        )

    def blend(self) -> None:
        for data_array in self.data_arrays:
            self.__add_data_array(data_array)

    @staticmethod
    def blend_func(data_arrays: list[xr.DataArray]) -> xr.DataArray:
        eqc_mean = EqcMean(data_arrays)
        eqc_mean.blend()
        return eqc_mean.as_data_array()
