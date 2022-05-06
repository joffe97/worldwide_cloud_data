import functools
import time

import dask.array
import xarray as xr
import numpy as np
from pyresample import AreaDefinition
from datetime import datetime
from enum import Enum, auto
import multiprocessing as mp
from multiprocessing import shared_memory
from typing import Optional, Type

from wwclouds.config import CPU_COUNT
from wwclouds.helpers.list_helper import ListHelper
from wwclouds.data_types.axis import Axis
from wwclouds.helpers.longitude_helper import LongitudeHelper
from wwclouds.helpers.latitude_helper import LatitudeHelper
from wwclouds.helpers.data_array_helper import DataArraysHelper
from wwclouds.helpers.math_helper import MathHelper
from wwclouds.helpers.axis_helper import AxisHelper


class LongitudeSection:
    def __init__(self,
                 data_array1: xr.DataArray,
                 from_longitude: float,
                 to_longitude: float,
                 *,
                 data_array2: Optional[xr.DataArray] = None
                 ):
        self.data_array1 = data_array1
        self.data_array2 = data_array2
        self.from_longitude = from_longitude
        self.to_longitude = to_longitude

    @property
    def middle_longitude(self) -> float:
        return LongitudeHelper.get_middle(self.from_longitude, self.to_longitude)

    @property
    def is_merged(self) -> bool:
        return self.data_array2 is not None

    @property
    def data_array(self) -> xr.DataArray:
        if self.is_merged:
            raise LookupError("cannot choose data_array, as there exists two different data_arrays")
        return self.data_array1

    @property
    def data_arrays(self) -> tuple[xr.DataArray, Optional[xr.DataArray]]:
        return self.data_array1, self.data_array2

    def merge_with_section(self, section: "LongitudeSection") -> list["LongitudeSection"]:
        if any(sec.is_merged for sec in (self, section)):
            return [self, section]
        elif self.to_longitude == section.from_longitude:
            section1, section2 = self, section
        elif section.to_longitude == self.from_longitude:
            section1, section2 = section, self
        else:
            return [self, section]

        intersection_lon = section1.to_longitude

        middle_lons = tuple(sec.middle_longitude for sec in (section1, section2))
        middle_intersection_diffs = tuple(map(
            lambda middle_lon: LongitudeHelper.get_diff(middle_lon, intersection_lon),
            middle_lons
        ))
        shortest_middle_intersection_diff_index = min(
            range(len(middle_intersection_diffs)),
            key=lambda index: middle_intersection_diffs[index]
        )

        if shortest_middle_intersection_diff_index == 0:
            new_intersections = middle_lons[0], LongitudeHelper.add(intersection_lon, middle_intersection_diffs[0])
        elif shortest_middle_intersection_diff_index == 1:
            new_intersections = LongitudeHelper.add(intersection_lon, -middle_intersection_diffs[1]), middle_lons[1]
        else:
            raise ValueError("diff index must be either 0 or 1")

        new_section1 = LongitudeSection(section1.data_array, section1.from_longitude, new_intersections[0])
        new_section_merge = LongitudeSection(section1.data_array, new_intersections[0], new_intersections[1], data_array2=section2.data_array)
        new_section2 = LongitudeSection(section2.data_array, new_intersections[1], section2.to_longitude)

        new_sections = [new_section1, new_section_merge, new_section2]
        new_sections_filtered = list(filter(lambda sec: sec.from_longitude != sec.to_longitude, new_sections))
        return new_sections_filtered


class MapPortion:
    def __init__(self, values: np.ndarray, lon_list: list[float], lat_list: list[float],
                 lon_indexes: list[int], lat_indexes: list[int]):
        self.values = values
        self.lon_list = lon_list
        self.lat_list = lat_list
        self.lon_indexes = lon_indexes
        self.lat_indexes = lat_indexes

    def __copy(self, **override_args) -> "MapPortion":
        args = {**vars(self), **override_args}
        return MapPortion(**args)

    def split_by_lat_axis(self, count: int) -> list["MapPortion"]:
        lat_indexes_list = ListHelper.split_list(self.lat_indexes, count)
        map_portions = [self.__copy(lat_indexes=lat_indexes) for lat_indexes in lat_indexes_list]
        return map_portions


class EqcBlend:
    def __init__(self,
                 latitude_range: tuple[float, float] = (-Axis.LAT.value // 2, Axis.LAT.value // 2),
                 merge_intensity: int = 60):
        self.latitude_range = tuple(sorted(latitude_range))
        self.merge_intensity = merge_intensity

        self.data_arrays = []
        self.lon_delta_step = None
        self.lat_delta_step = None
        self.shared_earth_array = None
        self.__earth_array = None

        self.__data_array_values_map = None
        self.__lon_lats = None

    def __call__(self, data_arrays: list[xr.DataArray]) -> xr.DataArray:
        if len(data_arrays) == 0:
            raise ValueError("cannot call EqcMean object with 0 DataArrays")
        self.__init_data_arrays(data_arrays)
        self.blend()
        return self.as_data_array()

    def __del__(self):
        self.shared_earth_array.close()
        self.shared_earth_array.unlink()

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
    def __data_type(self) -> type:
        return self.__get_values_from_data_array(self.__first_data_array)[0][0].dtype

    @property
    def __shape(self) -> (int, int):
        return self.lat_len, self.lon_len

    @property
    def time_range(self) -> tuple[datetime, datetime]:
        return DataArraysHelper.time_range(self.data_arrays)

    @property
    def lonlats(self):
        if self.__lon_lats is None:
            lats_sorted = self.__get_axis_sorted(Axis.LAT)
            lons = self.__get_axis_sorted(Axis.LON)
            lats = np.flip(lats_sorted)
            self.__lon_lats = lons, lats
        return self.__lon_lats

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

    @property
    def __earth_array_latitude_index_range(self) -> tuple[int, int]:
        top = self.__translate_coords_to_earth_array_indexes((0, self.latitude_range[0]))
        bot = self.__translate_coords_to_earth_array_indexes((0, self.latitude_range[1]))
        return top[1], bot[1]

    @property
    def __earth_array_latitude_length(self) -> int:
        min_index, max_index = self.__earth_array_latitude_index_range
        return max_index - min_index + 1

    def __init_data_arrays(self, data_arrays: list[xr.DataArray]):
        self.data_arrays = data_arrays
        self.lon_delta_step, self.lat_delta_step = self.__get_lonlats_delta_steps(20)

        self.shared_earth_array, self.__earth_array = self.__create_shared_earth_array()

    def __get_values_from_data_array(self, data_array: xr.DataArray) -> np.ndarray:
        if self.__data_array_values_map is None:
            self.__data_array_values_map = dict((id(data_array), data_array.values) for data_array in self.data_arrays)
        return self.__data_array_values_map[id(data_array)]

    def __create_shared_earth_array(self) -> (shared_memory.SharedMemory, np.ndarray):
        size = np.dtype(self.__data_type).itemsize * np.prod(self.lat_len * self.lon_len)
        shm = shared_memory.SharedMemory(create=True, size=size)
        dst = np.ndarray(self.__shape, self.__data_type, buffer=shm.buf)
        dst[:] = np.nan
        return shm, dst

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

    def __get_indexes_from_axis(self, axis_values: list[float], from_axis_val: float, to_axis_val: float,
                                axis_helper: Type[AxisHelper],
                                edge_size: int = 0,
                                max_length: Optional[int] = None) -> list[int]:
        indexes = []
        for index, value in enumerate(axis_values):
            if axis_helper.is_between(value, from_axis_val, to_axis_val):
                if len(indexes) == 0:
                    for i in range(edge_size):
                        indexes.append((index - (1 + i)) % len(axis_values))
                indexes.append(index)
            elif len(indexes) != 0:
                for i in range(edge_size):
                    indexes.append((index + i) % len(axis_values))
                break

        if max_length is not None and max_length < len(indexes):
            middle_axis_val = axis_helper.get_middle(from_axis_val, to_axis_val)
            start_index = 0
            end_index = len(indexes) - 1
            for _ in range(len(indexes) - max_length):
                start_val, end_val = (axis_values[indexes[i]] for i in (start_index, end_index))
                start_diff, end_diff = (axis_helper.get_diff(val, middle_axis_val) for val in (start_val, end_val))
                if start_diff > end_diff:
                    start_index += 1
                else:
                    end_index -= 1
            indexes = indexes[start_index:(end_index + 1)]

        return indexes

    def __longitude_section_to_map_portions(
            self,
            lon_section: LongitudeSection) -> list[tuple[MapPortion, Optional[MapPortion]]]:
        from_longitude = lon_section.from_longitude
        to_longitude = lon_section.to_longitude
        from_latitude, to_latitude = self.latitude_range
        lat_edge_size = 5
        lat_indexes_count = self.__earth_array_latitude_length + lat_edge_size * 2 - 2
        map_portion_lists = [[], []]
        for index, data_array in enumerate(lon_section.data_arrays[:2]):
            if data_array is None:
                map_portion_lists[index] = [None] * CPU_COUNT
                continue
            values = self.__get_values_from_data_array(data_array)
            area: AreaDefinition = data_array.attrs["area"]
            lons, lats = area.get_lonlats()
            longitude_list = lons[0]
            latitude_list = list(reversed(list(map(lambda val: val[0], lats))))
            lon_indexes = self.__get_indexes_from_axis(longitude_list, from_longitude, to_longitude, LongitudeHelper,
                                                       edge_size=1)
            lat_indexes = self.__get_indexes_from_axis(latitude_list, from_latitude, to_latitude, LatitudeHelper,
                                                       max_length=lat_indexes_count,
                                                       edge_size=lat_edge_size)
            map_portion = MapPortion(values, longitude_list, latitude_list, lon_indexes, lat_indexes)
            map_portion_lists[index] = map_portion.split_by_lat_axis(CPU_COUNT)
        map_portions = [tuple(map_tuples) for map_tuples in zip(*map_portion_lists)]
        return map_portions

    def __add_value_from_single_map_portion(self, map_portion: MapPortion) -> None:
        earth_array_lat_range = self.__earth_array_latitude_index_range
        for lat_index in map_portion.lat_indexes:
            for lon_index in map_portion.lon_indexes:
                value = map_portion.values[lat_index][lon_index]
                new_lon_index, new_lat_index = self.__translate_coords_to_earth_array_indexes(
                    (map_portion.lon_list[lon_index], map_portion.lat_list[lat_index])
                )
                if np.isnan(value) or not (earth_array_lat_range[0] <= new_lat_index <= earth_array_lat_range[1]):
                    continue
                self.__earth_array[new_lat_index][new_lon_index] = value

    def __add_value_from_two_map_portions(self, map_portion1: MapPortion, map_portion2: MapPortion) -> None:
        lon_indexes_length = len(map_portion1.lon_indexes)
        earth_array_lat_range = self.__earth_array_latitude_index_range
        if len(map_portion1.lat_indexes) != len(map_portion2.lat_indexes):
            raise ValueError("map_portion objects must have axis indexes of same dimensions")
        for lat_index1, lat_index2 in zip(map_portion1.lat_indexes, map_portion2.lat_indexes):
            for i, (lon_index1, lon_index2) in enumerate(zip(map_portion1.lon_indexes, map_portion2.lon_indexes)):
                new_lon_index, new_lat_index = self.__translate_coords_to_earth_array_indexes(
                    (map_portion1.lon_list[lon_index1], map_portion1.lat_list[lat_index1])
                )
                if not (earth_array_lat_range[0] <= new_lat_index <= earth_array_lat_range[1]):
                    continue
                values = [map_portion1.values[lat_index1][lon_index1], map_portion2.values[lat_index2][lon_index2]]
                values_filtered = list(filter(lambda val: val and not np.isnan(val), values))
                if len(values_filtered) == 0:
                    continue
                elif len(values_filtered) == 1:
                    value = values_filtered[0]
                else:
                    progress = ((i / lon_indexes_length) - 0.5) * self.merge_intensity  # Progress from -x to x, where 2x is merge_intensity
                    weight = MathHelper.sigmoid(progress)
                    value = values_filtered[0] * (1 - weight) + values_filtered[1] * weight
                self.__earth_array[new_lat_index][new_lon_index] = value

    def __add_value_from_map_portions(self, map_portion1: MapPortion, map_portion2: Optional[MapPortion]) -> None:
        if map_portion2 is None:
            self.__add_value_from_single_map_portion(map_portion1)
        else:
            self.__add_value_from_two_map_portions(map_portion1, map_portion2)

    def __add_lon_section(self, lon_section: LongitudeSection) -> None:
        map_portions_list = self.__longitude_section_to_map_portions(lon_section)
        processes = [
            mp.Process(
                target=self.__add_value_from_map_portions,
                args=(map_portion1, map_portion2)
            ) for map_portion1, map_portion2 in map_portions_list
        ]
        for methodname in ["start", "join", "close"]:
            for process in processes:
                getattr(process, methodname)()

    def __get_data_array_edge_longitudes(self) -> list[float]:
        lons_sorted = sorted(data_array.attrs["area"].proj_dict["lon_0"] for data_array in self.data_arrays)
        edge_longitudes = []
        for index in range(len(lons_sorted)):
            lon_low = lons_sorted[index]
            lon_high = lons_sorted[(index + 1) % len(lons_sorted)]
            lon_middle = LongitudeHelper.get_middle(lon_low, lon_high)
            edge_longitudes.append(lon_middle)
        return sorted(edge_longitudes)

    def __get_data_array_between_longitudes(self, lon_a: float, lon_b: float) -> xr.DataArray:
        lon_middle = LongitudeHelper.get_middle(lon_a, lon_b)
        lowest_lon_diff = np.inf
        data_array_between = None
        for data_array in self.data_arrays:
            cur_lon_diff = LongitudeHelper.get_diff(data_array.attrs["area"].proj_dict["lon_0"], lon_middle)
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
            self.__earth_array.copy(),
            dims=["y", "x"],
            coords=self.coords,
            attrs=attrs
        )

    def get_longitude_sections(self) -> list[LongitudeSection]:
        lon_sections_unmerged = []
        lon_edges = self.__get_data_array_edge_longitudes()
        for index in range(len(lon_edges)):
            lon_edge1 = lon_edges[index]
            lon_edge2 = lon_edges[(index + 1) % len(lon_edges)]
            data_array = self.__get_data_array_between_longitudes(lon_edge1, lon_edge2)
            longitude_section = LongitudeSection(data_array, lon_edge1, lon_edge2)
            lon_sections_unmerged.append(longitude_section)

        lon_sections_merged = []
        lon_section_a = lon_sections_unmerged[0]
        for lon_section_b in lon_sections_unmerged[1:]:
            new_sections = lon_section_a.merge_with_section(lon_section_b)
            lon_section_a = new_sections[-1]
            lon_sections_merged.extend(new_sections[:-1])
        edge_sections = lon_section_a.merge_with_section(lon_sections_merged[0])
        lon_sections_merged[0] = edge_sections[-1]
        lon_sections_merged.extend(edge_sections[:-1])
        return lon_sections_merged

    def blend(self) -> None:
        lon_sections = self.get_longitude_sections()
        for lon_section in lon_sections:
            self.__add_lon_section(lon_section)
