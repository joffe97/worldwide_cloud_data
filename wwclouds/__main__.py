import os
import sys
import argparse
import time
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
from image_visual.image_visual import ImageVisual
from video_maker.video_maker import VideoMaker


class System:
    def __init__(self, product_enum: ProductEnum, utctime: datetime, resolution: int,
                 hours: int = None, images_per_hour: int = None, fps: int = None, **kwargs):
        self.product_enum = product_enum
        self.utctime = utctime
        self.resolution = resolution
        self.hours = hours
        self.images_per_hour = images_per_hour
        self.fps = fps

        self._frequencies = [10.6]   # TODO: Find necessary frequencies.
        self._legal_resolutions = [500, 1000, 2000, 3000.403165817]
        self._max_latitude = 70
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
            "output",
            help="the desired output",
            choices=[product_enum.name.lower() for product_enum in ProductEnum],
            nargs="+"
        )
        parser.add_argument(
            "resolution",
            help="resolution of the product",
            type=int
        )
        parser.add_argument(
            "--utctime",
            help="timestamp (defaults to current time)",
            default=datetime.utcnow().timestamp(),
            type=int
        )
        parser.add_argument(
            "--hours",
            help="hours of video, going backwards (only applicable to video output)",
            type=int
        )
        parser.add_argument(
            "--iph",
            help="images per hour (only applicable to video output)",
            type=int
        )
        parser.add_argument(
            "--fps",
            help="frames per second (only applicable to video output)",
            type=int
        )
        args = parser.parse_args()
        args_dict = {**vars(args), **override_kwargs}

        product_enum = ProductEnum.from_str_list(args_dict["output"])
        utctime = datetime.fromtimestamp(args_dict["utctime"])
        resolution = int(args_dict["resolution"])
        hours = int(args_dict["hours"]) if args_dict["hours"] is not None else None
        images_per_hour = int(args_dict["iph"]) if args_dict["iph"] is not None else None
        fps = int(args_dict["fps"]) if args_dict["fps"] is not None else None

        if product_enum & ProductEnum.VIDEO:
            illegal_args = [arg for arg in ("hours", "iph", "fps") if args_dict[arg] is None or args_dict[arg] <= 0]
            if illegal_args:
                parser.error(f"video output cannot be created without the following arguments, "
                             f"where integers must be larger than 0: {illegal_args}")

        return System(product_enum, utctime, resolution, hours, images_per_hour, fps)

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
        return f"{self.__product_directory_path}/video_h{self.hours}_iph{self.images_per_hour}_fps{self.fps}.mp4"

    def __copy(self, **override_args) -> "System":
        args = {**vars(self), **override_args}
        return System(**args)

    def __get_imagedata_path_for_format(self, file_ending: str) -> str:
        return f"{self.__product_directory_path}/imagedata.{file_ending}"

    def __get_combined_scene(self) -> SceneExt:
        file_readers = self._satellite_collection.download_all(frequencies=self._frequencies, utctime=self.utctime)
        scenes = [reader.read_to_scene() for reader in file_readers]
        multi_scn_ext = MultiSceneExt(scenes)
        multi_scn_ext.load(self._frequencies, resolution=self._legal_resolutions)
        multi_scn_ext_eqc = multi_scn_ext.resample_loaded_to_eqc(self.resolution)
        comb_scene = multi_scn_ext_eqc.combine(self._max_latitude)
        return comb_scene

    def __create_new_image(self) -> XRImage:
        comb_scene = self.__get_combined_scene()
        compositor = CloudCompositor("clouds", 230, transition_gamma=1.5)
        composite = compositor([comb_scene[frequency] for frequency in self._frequencies])
        return to_image(composite)

    def __get_existing_image(self) -> XRImage:
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
            world_map, image = ImageVisual.from_image_path(image_path, load=True)
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
        image_paths.reverse()
        return image_paths

    def __create_video(self):
        image_paths = self.__get_image_paths_for_video(self.hours, self.images_per_hour)
        VideoMaker(self.__video_path, image_paths, self.fps).create_video()

    def create_products(self):
        start_time = time.time()
        print("Stating product creation")
        print("Creating imagedata")
        self.__create_imagedata_for_products()
        if self.product_enum & (ProductEnum.IMAGEVISUAL | ProductEnum.VIDEO):
            print("Creating imagevisual")
            self.__create_imagevisual()
        if self.product_enum & ProductEnum.VIDEO:
            print("Creating video")
            self.__create_video()
        print(f"Finished in {time.time() - start_time} seconds")


if __name__ == '__main__':
    System.from_args(
        # utctime=datetime(2022, 2, 14, 9, 25).timestamp()
        # utctime=datetime.utcnow().timestamp()
        utctime=datetime(2020, 12, 14, 19, 00).timestamp()
    ).create_products()
