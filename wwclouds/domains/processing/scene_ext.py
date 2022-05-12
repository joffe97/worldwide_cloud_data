import functools
from typing import Callable, Union

import matplotlib.pyplot as plt
import satpy.writers
from pyresample import create_area_def, AreaDefinition
from satpy import Scene
from satpy.writers import to_image
from satpy.composites import CloudCompositor
from trollimage.xrimage import XRImage
from xarray import DataArray

from wwclouds.config import DATA_PATH_SATPY_RESAMPLE_CACHE


def _return_as_scene_ext_decorator(func) -> Callable[..., "SceneExt"]:
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        scene = func(*args, **kwargs)
        return SceneExt.from_scene(scene)

    return wrapper


class SceneExt(Scene):
    def __init__(self, filenames=None, reader=None, filter_parameters=None,
                 reader_kwargs=None):
        super().__init__(filenames, reader, filter_parameters, reader_kwargs)

    @staticmethod
    def from_scene(scene: Scene) -> "SceneExt":
        scene_ext = SceneExt()
        for key, value in vars(scene).items():
            setattr(scene_ext, key, value)
        return scene_ext

    @property
    def __first_band(self) -> DataArray:
        return next(band for band in self)

    @property
    def area(self) -> AreaDefinition:
        if not self.all_same_area:
            raise ValueError("the area property cannot be called, as the scene contains multiple areas")
        return self.__first_band.attrs["area"]

    @property
    def proj(self) -> dict:
        if not self.all_same_proj:
            raise ValueError("the proj property cannot be called, as the scene contains multiple projections")
        return self.__first_band.attrs["area"].proj_dict

    @property
    def lon_0(self) -> Union[float, None]:
        return self.proj.get("lon_0")

    @_return_as_scene_ext_decorator
    def resample(self, destination=None, datasets=None, generate=True,
                 unload=True, resampler=None, reduce_data=True,
                 **resample_kwargs) -> "SceneExt":
        return super().resample(destination, datasets, generate, unload, resampler, reduce_data, **resample_kwargs)

    def __imshow_wavelength(self, wavelength: float) -> None:
        plt.figure()
        plt.imshow(self[wavelength])
        plt.show()

    def __imshow_composite(self, composite: str) -> None:
        img = satpy.writers.get_enhanced_image(self[composite])
        plt.figure()
        img.data.plot.imshow()
        plt.show()

    def imshow(self, query: Union[str, float]):
        if True or query in self.available_dataset_names():
            self.__imshow_wavelength(query)
        elif False or query in self.available_composite_names():
            self.__imshow_composite(query)
        else:
            raise TypeError("query is of an incompatible type")

    def resample_to_eqc_area(self, *, resolution=None, **kwargs) -> "SceneExt":
        projection = {"proj": "eqc", "lon_0": self.lon_0}  # Equidistant cylindrical projection

        area_def_args = dict()
        if resolution is not None:
            area_def_args["resolution"] = resolution

        area_def = create_area_def(
            area_id="eqc_area",
            projection=projection,
            **area_def_args
        )
        return self.resample(
            destination=area_def,
            resampler="bilinear",
            cache_dir=DATA_PATH_SATPY_RESAMPLE_CACHE,
            **kwargs
        )

    def create_cloud_image(self, frequencies: list[float]) -> XRImage:
        compositor = CloudCompositor(name="clouds", transition_min=230.0, transition_max=298.15, transition_gamma=1.5)
        composite = compositor([self[frequency] for frequency in frequencies])
        return to_image(composite)
