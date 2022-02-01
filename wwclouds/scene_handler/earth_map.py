import numpy as np
from PIL import Image
from matplotlib import pyplot as plt
from satpy import Scene
from xarray import DataArray
from wwclouds.scene_handler.scene_ext import SceneExt


class EarthMap:
    def __init__(self, step_size: float, band):
        self.band = band
        self.step_size = step_size

        self.lat_len = int(180 // step_size)
        self.lon_len = int(360 // step_size)

        self.earth_array = np.empty((self.lat_len, self.lon_len))
        self.earth_array[:] = np.nan

    def translate_coords_to_earth_array_indexes(self, coord: tuple[float, float]) -> tuple[int, int]:
        if len(coord) != 2:
            raise ValueError("coord cannot contain more than two values")
        coord = list(coord)
        earth_array_pos = [-1, -1]
        coord[0] = (coord[0] + 180) % 360
        coord[1] = (coord[1] + 90) % 180
        for index, coord_part in enumerate(coord):
            earth_array_pos[index] = round(coord_part / self.step_size)
        earth_array_pos[0] %= self.lon_len
        earth_array_pos[1] %= self.lat_len
        return tuple(earth_array_pos)

    def add_value_to_coordinate(self, value: float, coordinate: tuple[float, float]):
        if np.isnan(value):
            return
        lon_index, lat_index = self.translate_coords_to_earth_array_indexes(coordinate)
        old_value = value if np.isnan(self.earth_array[lat_index][lon_index]) else self.earth_array[lat_index][lon_index]
        self.earth_array[lat_index][lon_index] = (old_value + value) / 2

    def add_values(self, values: np.ndarray, lonlat: tuple[np.ndarray, np.ndarray]):
        longitude = lonlat[0][0]
        latitude = list(reversed(list(map(lambda val: val[0], lonlat[1]))))
        for lat_index, value_row in enumerate(values):
            for lon_index, value in enumerate(value_row):
                coord = (longitude[lon_index], latitude[lat_index])
                self.add_value_to_coordinate(value, coord)

    def add_values_from_scene_ext(self, scene_ext: SceneExt):
        self.add_values(scene_ext.get_values(self.band), scene_ext.get_lonlats(self.band))

    def imshow(self):
        plt.figure()
        plt.imshow(self.earth_array)
        plt.show()
