import os
import sys
import argparse
import warnings
from datetime import datetime

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


class System:
    def __init__(self, product_enum: ProductEnum, utctime: datetime, resolution: int):
        self.product_enum = product_enum
        self.utctime = utctime
        self.resolution = resolution

        self.frequencies = [12.3]   # TODO: Find necessary frequencies.
        self.satellite_collection = SatelliteCollection([
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
            choices=[range(2000, 400_000)]  # TODO: Set default resolution. Find resolution range. Should this arg be required?
        )
        args = parser.parse_args()
        args_dict = {**vars(args), **override_kwargs}

        product_enum = ProductEnum.from_str_list(args_dict["output"])
        utctime = datetime.fromtimestamp(args_dict["utctime"])
        resolution = args_dict["resolution"]

        return System(product_enum, utctime, resolution)

    @property
    def time_range(self) -> str:
        return self.satellite_collection.get_scan_times_str(self.frequencies, self.utctime)

    @property
    def product_directory_path(self) -> str:
        return f"{DATA_PATH_PRODUCT}/{self.time_range}/{self.resolution}"

    @property
    def imagevisual_path(self) -> str:
        return f"{self.product_directory_path}/imagevisual.png"

    def get_imagedata_path_for_format(self, file_ending: str) -> str:
        return f"{self.product_directory_path}/imagedata.{file_ending}"

    def get_comb_scene(self) -> SceneExt:
        file_readers = self.satellite_collection.download_all(frequencies=self.frequencies, utctime=self.utctime)
        scenes = [reader.read_to_scene() for reader in file_readers]
        multi_scn_ext = MultiSceneExt(scenes)
        multi_scn_ext.load(self.frequencies, resolution=[500, 1000, 2000, 3000.403165817])  # TODO: Find legal resolutions
        comb_scene = multi_scn_ext.combine(resolution=self.resolution)
        return comb_scene

    def create_image(self):
        compositor = CloudCompositor("clouds")
        comb_scene = self.get_comb_scene()
        composite = compositor([comb_scene[frequency] for frequency in self.frequencies])
        return to_image(composite)

    def get_existing_image(self):
        with xarray.open_rasterio(self.get_imagedata_path_for_format("tif"), parse_coordinates=True) as dataset:
            if "bands" not in dataset.dims:
                dataset = dataset.swap_dims({"band": "bands"})
                dataset.coords["bands"] = ["L", "A"]
                del dataset.coords["band"]
        return XRImage(dataset)

    def get_image(self) -> XRImage:
        if not os.path.exists(self.get_imagedata_path_for_format("tif")):
            return self.create_image()
        else:
            return self.get_existing_image()

    def create_imagedata(self, file_formats: list[str]) -> None:
        img = self.get_image()
        os.makedirs(self.product_directory_path, exist_ok=True)
        for file_format in file_formats:
            if os.path.exists(filepath := self.get_imagedata_path_for_format(file_format)):
                continue
            img.save(filepath)

    def create_imagedata_for_products(self) -> None:
        file_formats = ["tif"]
        if self.product_enum & (ProductEnum.IMAGEVISUAL | ProductEnum.VIDEO):
            file_formats.append("png")
        self.create_imagedata(file_formats)

    def create_imagevisual(self) -> None:
        image_path = self.get_imagedata_path_for_format("png")
        world_map, image = WorldMap.from_image_path(image_path, load=True)
        world_map.add_4dim_image(image)
        world_map.save_as_png(self.imagevisual_path)

    def create_video(self):
        pass

    def create_products(self):
        self.create_imagedata_for_products()
        if self.product_enum & ProductEnum.IMAGEVISUAL:
            self.create_imagevisual()
        if self.product_enum & ProductEnum.VIDEO:
            self.create_video()


if __name__ == '__main__':
    system = System.from_args(utctime=datetime(2022, 2, 14, 9, 25).timestamp(), resolution=60_000)
    system.create_products()
