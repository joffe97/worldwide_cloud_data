import statistics
import time

import xarray as xr
import dask.array as da
import numpy as np
from pyresample import AreaDefinition
from datetime import datetime
from enum import Enum, auto
from geopy import distance
from typing import Iterator


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
    def degree_count(self):
        if self == Axis.LON:
            return 360
        elif self == Axis.LAT:
            return 180
        else:
            raise ValueError("degree_count is not implemented for the given axis")


class EqcBlend:
    def __init__(self, data_arrays: list[xr.DataArray]):
        if len(data_arrays) == 0:
            raise ValueError("cannot create EqcMean object with 0 DataArrays")
        self.data_arrays = data_arrays
        self.lon_delta_step, self.lat_delta_step = self.__get_lonlats_delta_steps(20)

        self.earth_array = np.empty((self.lat_len, self.lon_len))
        self.earth_array[:] = np.nan

    @property
    def lon_len(self) -> int:
        return int(Axis.LON.degree_count // self.lon_delta_step)

    @property
    def lat_len(self) -> int:
        return int(Axis.LAT.degree_count // self.lat_delta_step)

    @property
    def __first_data_array(self) -> xr.DataArray:
        return self.data_arrays[0]

    @property
    def __data_arrays_sorted_by_longitude(self) -> list[xr.DataArray]:
        return sorted(
            self.data_arrays,
            key=lambda data_array: data_array.attrs["area"].proj_dict["lon_0"]
        )

    @property
    def time_range(self) -> tuple[datetime, datetime]:
        print()
        for data_array in self.data_arrays:
            print(data_array.attrs["start_time"], data_array.attrs["end_time"])
        print()
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
            "y": lats,
            "x": lons
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
        area: AreaDefinition = self.__first_data_array.attrs["area"]
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
        max_value = axis.degree_count
        delta_step = getattr(self, f"{axis.name.lower()}_delta_step")
        axis_len_aim = getattr(self, f"{axis.name.lower()}_len")

        axis = np.arange(0, max_value, delta_step)
        overflow = len(axis) - axis_len_aim
        if overflow < 0:
            ValueError("axis cannot be less than the aim")
        axis.resize(axis_len_aim)
        return axis

    def __get_axis_sorted(self, axis: Axis) -> np.ndarray:
        axis_len_aim = getattr(self, f"{axis.name.lower()}_len")
        delta_step = (axis.length_in_metres // 2) // axis_len_aim

        axis = np.arange(-axis.length_in_metres // 2, axis.length_in_metres // 2, delta_step)
        overflow = len(axis) - axis_len_aim
        if overflow < 0:
            ValueError("axis cannot be less than the aim")
        axis.resize(axis_len_aim)
        return axis

    def __get_lonlats_delta_steps(self, max_samples: int) -> tuple[float, float]:
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
            raise ValueError("coord must contain two values")
        aligned_coord = [
            (coord[0] + (Axis.LON.degree_count // 2)) % Axis.LON.degree_count,
            (coord[1] + (Axis.LAT.degree_count // 2)) % Axis.LAT.degree_count
        ]
        earth_array_indexes = (
            round(aligned_coord[0] / self.lon_delta_step) % self.lon_len,
            round(aligned_coord[1] / self.lat_delta_step) % self.lat_len
        )
        return earth_array_indexes

    @staticmethod
    def __get_lon_diff(lon_a: float, lon_b: float) -> float:
        lon_low, lon_high = sorted((lon_a, lon_b))
        lon_diff = lon_high - lon_low
        return min(lon_diff, Axis.LON.degree_count - lon_diff)

    @staticmethod
    def __get_lon_middle(lon_a: float, lon_b: float) -> float:
        lon_diff = EqcBlend.__get_lon_diff(lon_a, lon_b)
        lon_middles = []
        for lon in lon_a, lon_b:
            cur_lon_middle = lon + (lon_diff / 2)
            if cur_lon_middle > Axis.LON.degree_count / 2:
                cur_lon_middle -= Axis.LON.degree_count
            lon_middles.append(cur_lon_middle)
        lon_middle = min(
            lon_middles,
            key=lambda lon: EqcBlend.__get_lon_diff(lon_a, lon) + EqcBlend.__get_lon_diff(lon_b, lon)
        )
        return lon_middle

    @staticmethod
    def __is_between_longitudes(lon: float, lon_a: float, lon_b: float) -> bool:
        range_width = EqcBlend.__get_lon_diff(lon_a, lon_b)
        lon_middle = EqcBlend.__get_lon_middle(lon_a, lon_b)
        return EqcBlend.__get_lon_diff(lon_middle, lon) <= range_width / 2

    def __add_data_array(self, data_array: xr.DataArray, from_longitude: float, to_longitude: float) -> None:
        values = data_array.values
        area: AreaDefinition = data_array.attrs["area"]
        lons, lats = area.get_lonlats()
        longitude_list = lons[0]
        latitude_list = list(reversed(list(map(lambda val: val[0], lats))))

        lon_indexes = []
        for index, lon in enumerate(longitude_list):
            if self.__is_between_longitudes(lon, from_longitude, to_longitude):
                if len(lon_indexes) == 0:
                    lon_indexes.append((index - 1) % len(longitude_list))
                lon_indexes.append(index)
            elif len(lon_indexes) != 0:
                lon_indexes.append(index)
                break

        for lat_index, value_row in enumerate(values):
            for lon_index in lon_indexes:
                value = value_row[lon_index]
                if np.isnan(value):
                    continue
                new_lon_index, new_lat_index = self.__translate_coords_to_earth_array_indexes(
                    (longitude_list[lon_index], latitude_list[lat_index])
                )
                self.earth_array[new_lat_index][new_lon_index] = value

    def __get_data_array_edge_longitudes(self) -> list[float]:
        lons_sorted = sorted(data_array.attrs["area"].proj_dict["lon_0"] for data_array in self.data_arrays)
        edge_longitudes = []
        for index in range(len(lons_sorted)):
            lon_low = lons_sorted[index]
            lon_high = lons_sorted[(index + 1) % len(lons_sorted)]
            lon_middle = self.__get_lon_middle(lon_low, lon_high)
            edge_longitudes.append(lon_middle)
        return sorted(edge_longitudes)

    def __get_data_array_between_longitudes(self, lon_a: float, lon_b: float) -> xr.DataArray:
        lon_middle = self.__get_lon_middle(lon_a, lon_b)
        lowest_lon_diff = np.inf
        data_array_between = None
        for data_array in self.data_arrays:
            cur_lon_diff = self.__get_lon_diff(data_array.attrs["area"].proj_dict["lon_0"], lon_middle)
            if cur_lon_diff < lowest_lon_diff:
                lowest_lon_diff = cur_lon_diff
                data_array_between = data_array
        return data_array_between

    def as_data_array(self) -> xr.DataArray:
        start_time, end_time = self.time_range
        attrs = {
            "start_time": start_time,
            "end_time": end_time,
            "area": self.area_def
        }
        return xr.DataArray(
            self.earth_array,
            dims=["y", "x"],
            coords=self.coords,
            attrs=attrs
        )

    def blend(self) -> None:
        lon_edges = self.__get_data_array_edge_longitudes()
        for index in range(len(lon_edges)):
            print("BAM!")
            edge1 = lon_edges[index]
            edge2 = lon_edges[(index + 1) % len(lon_edges)]
            data_array = self.__get_data_array_between_longitudes(edge1, edge2)
            self.__add_data_array(data_array, edge1, edge2)

    @staticmethod
    def blend_func(data_arrays: list[xr.DataArray]) -> xr.DataArray:
        eqc_mean = EqcBlend(data_arrays)
        eqc_mean.blend()
        return eqc_mean.as_data_array()
