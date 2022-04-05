import os
import sys
import argparse
import warnings
from datetime import datetime, timedelta

import xarray
from satpy.writers import to_image
from satpy.composites import CloudCompositor
from trollimage.xrimage import XRImage

from config import ROOT_DIR
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

if not sys.warnoptions:
    warnings.simplefilter("ignore")

from satellite.satellite_enum import SatelliteEnum
from satellite.satellite_collection import SatelliteCollection
from scene_handler.multiscene_ext import MultiSceneExt
from scene_handler.scene_ext import SceneExt
from product_enum import ProductEnum
from config import DATA_PATH_PRODUCT
from world_map.world_map import WorldMap
from video_maker.video_maker import VideoMaker


class System:
    def __init__(self, product_enum: ProductEnum, utctime: datetime, resolution: int, **kwargs):
        self.product_enum = product_enum
        self.utctime = utctime
        self.resolution = resolution

        self._frequencies = [12.3]   # TODO: Find necessary frequencies.
        self._satellite_collection = SatelliteCollection([
            SatelliteEnum.METEOSAT8,
            SatelliteEnum.METEOSAT11,
            SatelliteEnum.HIMAWARI8,
            SatelliteEnum.GOES16,
            SatelliteEnum.GOES17
        ])

    @staticmethod
    def from_args(**override_kwargs) -> "System":
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "--output",
            help="The desired output",
            choices=[product_enum.name.lower() for product_enum in ProductEnum],
            required=True,
            nargs="+"
        )
        parser.add_argument(
            "--utctime",
            help="Timestamp (defaults to current time)",
            default=datetime.utcnow().timestamp()
        )
        parser.add_argument(
            "--resolution",
            help="Resolution of the product",
            required=True,
            type=int
        )
        args = parser.parse_args()
        args_dict = {**vars(args), **override_kwargs}

        product_enum = ProductEnum.from_str_list(args_dict["output"])
        utctime = datetime.fromtimestamp(args_dict["utctime"])
        resolution = int(args_dict["resolution"])

        return System(product_enum, utctime, resolution)

    @property
    def __time_subfolder(self) -> str:
        return "/".join(self._satellite_collection.get_scan_times_strings(self._frequencies, self.utctime))

    @property
    def __product_directory_path(self) -> str:
        return f"{DATA_PATH_PRODUCT}/{self.__time_subfolder}/{self.resolution}"

    @property
    def imagevisual_path(self) -> str:
        return f"{self.__product_directory_path}/imagevisual.png"

    @property
    def __video_path(self) -> str:
        return f"{self.__product_directory_path}/video.mp4"

    def __copy(self, **override_args) -> "System":
        args = {**vars(self), **override_args}
        return System(**args)

    def __get_imagedata_path_for_format(self, file_ending: str) -> str:
        return f"{self.__product_directory_path}/imagedata.{file_ending}"

    def __get_comb_scene(self) -> SceneExt:
        file_readers = self._satellite_collection.download_all(frequencies=self._frequencies, utctime=self.utctime)
        scenes = [reader.read_to_scene() for reader in file_readers]
        multi_scn_ext = MultiSceneExt(scenes)
        multi_scn_ext.load(self._frequencies, resolution=[500, 1000, 2000, 3000.403165817])  # TODO: Find legal resolutions
        comb_scene = multi_scn_ext.combine(resolution=self.resolution)
        return comb_scene

    def __create_new_image(self):
        compositor = CloudCompositor("clouds", 230, transition_gamma=1.5)
        comb_scene = self.__get_comb_scene()
        composite = compositor([comb_scene[frequency] for frequency in self._frequencies])
        return to_image(composite)

    def __get_existing_image(self):
        with xarray.open_rasterio(self.__get_imagedata_path_for_format("tif"), parse_coordinates=True) as dataset:
            if "bands" not in dataset.dims:
                dataset = dataset.swap_dims({"band": "bands"})
                dataset.coords["bands"] = ["L", "A"]
                del dataset.coords["band"]
            return XRImage(dataset)

    def __get_image(self) -> XRImage:
        if not os.path.exists(self.__get_imagedata_path_for_format("tif")):
            return self.__create_new_image()
        else:
            return self.__get_existing_image()

    def __create_imagedata(self, file_formats: list[str]) -> None:
        img = self.__get_image()
        os.makedirs(self.__product_directory_path, exist_ok=True)
        for file_format in file_formats:
            if os.path.exists(filepath := self.__get_imagedata_path_for_format(file_format)):
                continue
            img.save(filepath)

    def __create_imagedata_for_products(self) -> None:
        file_formats = ["tif"]
        if self.product_enum & (ProductEnum.IMAGEVISUAL | ProductEnum.VIDEO):
            file_formats.append("png")
        self.__create_imagedata(file_formats)

    def __create_imagevisual(self) -> str:
        if not os.path.exists(self.imagevisual_path):
            image_path = self.__get_imagedata_path_for_format("png")
            world_map, image = WorldMap.from_image_path(image_path, load=True)
            world_map.add_4dim_image(image)
            world_map.save_as_png(self.imagevisual_path)
        return self.imagevisual_path

    def __get_image_paths_for_video(self, hours: int, frames_per_hour: int) -> list[str]:
        image_paths = []
        for hour in range(hours):
            for hour_frame in range(frames_per_hour):
                minute = int((60 / frames_per_hour) * hour_frame)
                time_stamp = self.utctime - timedelta(hours=hour, minutes=minute)
                system = self.__copy(product_enum=ProductEnum.IMAGEVISUAL, utctime=time_stamp)
                system.create_products()
                image_paths.append(system.imagevisual_path)
        return image_paths

    def __create_video(self):
        hours = 24
        frames_per_hour = 1
        fps = 6

        image_paths = self.__get_image_paths_for_video(hours, frames_per_hour)
        VideoMaker(self.__video_path, image_paths, fps).create_video()

    def create_products(self):
        self.__create_imagedata_for_products()
        if self.product_enum & (ProductEnum.IMAGEVISUAL | ProductEnum.VIDEO):
            self.__create_imagevisual()
        if self.product_enum & ProductEnum.VIDEO:
            self.__create_video()


if __name__ == '__main__':
    system = System.from_args(utctime=datetime(2022, 2, 14, 9, 25).timestamp())
    # system = System.from_args(utctime=datetime(2022, 4, 5, 12, 25).timestamp())
    system.create_products()
