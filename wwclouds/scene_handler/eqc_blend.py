import statistics
import time

import xarray as xr
import dask.array as da
import numpy as np
from pyresample import AreaDefinition
from datetime import datetime
from enum import Enum, auto
from geopy import distance


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
        self.lon_delta_step, self.lat_delta_step = self.__get_lonlats_delta_steps(10)

        self.data = np.empty((self.lat_len, self.lon_len, len(self.data_arrays)))
        self.data[:] = np.nan

    @property
    def lon_len(self) -> int:
        return int(Axis.LON.degree_count // self.lon_delta_step)

    @property
    def lat_len(self) -> int:
        return int(Axis.LAT.degree_count // self.lat_delta_step)

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

    # (0,0)     -> (-180,90)
    # (100,100) -> (-90,45)
    # (200,200) -> (0,0)
    # (300,100) -> (90,45)
    def __translate_earth_array_indexes_to_coords(self, earth_array_indexes: tuple[int, int]) -> tuple[float, float]:
        if len(earth_array_indexes) != 2:
            raise ValueError("earth_array_indexes must contain two values")
        coords = {
            Axis.LON.name: (earth_array_indexes[0] - (self.lon_len / 2)) * self.lon_delta_step,
            Axis.LAT.name: (earth_array_indexes[1] - (self.lat_len / 2)) * self.lat_delta_step
        }
        for axis in Axis:
            edge_value = float(axis.degree_count / 2)
            coords[axis.name] = sorted([-edge_value, coords[axis.name], edge_value])[1]
        return coords[Axis.LON.name], coords[Axis.LAT.name]

    def __add_value_to_coordinate(self, value: float, coordinate: tuple[float, float]):
        if np.isnan(value):
            return
        lon_index, lat_index = self.__translate_coords_to_earth_array_indexes(coordinate)
        # old_value = value if np.isnan(self.data[lat_index][lon_index]) else self.data[lat_index][lon_index]
        # self.data[lat_index][lon_index] = (old_value + value) / 2

    def __add_data_array(self, data_array_indexes: int) -> None:
        data_array = self.data_arrays[data_array_indexes]
        values = data_array.values
        area: AreaDefinition = data_array.attrs["area"]
        lons, lats = area.get_lonlats()
        longitude_list = lons[0]
        latitude_list = list(reversed(list(map(lambda val: val[0], lats))))
        for lat_index, value_row in enumerate(values):
            for lon_index, value in enumerate(value_row):
                if np.isnan(value):
                    continue
                new_lon_index, new_lat_index = self.__translate_coords_to_earth_array_indexes(
                    (longitude_list[lon_index], latitude_list[lat_index])
                )
                self.data[new_lat_index][new_lon_index][data_array_indexes] = value

    def __estimate_value_in_earth_array(self, indexes: tuple[int, int]) -> float:
        values = self.data[indexes[1]][indexes[0]]
        lon, _ = self.__translate_earth_array_indexes_to_coords(indexes)
        # print(f"{indexes} -> {coords}")
        # shifted_coords = (indexes[0] - 90, indexes[1])
        # distance_values: list[tuple[float, float]] = []
        lowest_distance = np.inf
        value = np.nan
        for data_array, cur_value in zip(self.data_arrays, values):
            if np.isnan(cur_value):
                continue
            proj_center_lon = float(data_array.attrs["area"].proj_dict["lon_0"])
            lon_distance = abs(lon - proj_center_lon) % (Axis.LON.degree_count / 2)
            # distance_values.append((lon_distance, cur_value))
            # (lowest_distance, value) = min((lowest_distance, value), (lon_distance, cur_value), key=lambda tup: tup[0])
            if lon_distance < lowest_distance:
                lowest_distance = lon_distance
                value = cur_value
        return value

    def as_2d_np_array(self) -> np.ndarray:
        np_array = np.empty((self.lat_len, self.lon_len))
        np_array[:] = np.nan
        for lat_index, data_row in enumerate(self.data):
            print(f"\t* {lat_index}/{len(self.data)}")
            for lon_index in range(len(data_row)):
                np_array[lat_index][lon_index] = self.__estimate_value_in_earth_array((lon_index, lat_index))
        return np_array

    def as_data_array(self) -> xr.DataArray:
        np_array = da.from_array(self.as_2d_np_array())
        start_time, end_time = self.time_range
        attrs = {
            "start_time": start_time,
            "end_time": end_time,
            "area": self.area_def
        }
        return xr.DataArray(
            np_array,
            dims=["y", "x"],
            coords=self.coords,
            attrs=attrs
        )

    def blend(self) -> None:
        for data_array_index in range(len(self.data_arrays)):
            self.__add_data_array(data_array_index)

    @staticmethod
    def blend_func(data_arrays: list[xr.DataArray]) -> xr.DataArray:
        from time import time
        start_time = time()
        print(f"starting blending")
        eqc_mean = EqcBlend(data_arrays)
        eqc_mean.blend()
        print(f"inner blend: {time() - start_time}")
        return eqc_mean.as_data_array()
