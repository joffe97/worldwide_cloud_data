from satpy import Scene, MultiScene, DataQuery, DataID
from satpy.dataset.dataid import WavelengthRange
from pyresample import create_area_def, AreaDefinition
from typing import Callable
import functools
from collections.abc import Iterable
from xarray import DataArray
from wwclouds.scene_handler.scene_ext import SceneExt
from wwclouds.scene_handler.multiscene_blend import EqcMean


class MultiSceneExt(MultiScene):
    def __init__(self, scenes: list[Scene] | list[SceneExt] = None):
        super().__init__(self.__to_scenes_ext(scenes))
        self.loaded: list[str | float | int] = []

    @property
    def scenes_ext_sorted_by_longitude(self) -> list[SceneExt]:
        return list(sorted(self.scenes, key=lambda scn: scn.lon_0))

    @property
    def area_extent_ll(self) -> tuple[float, float, float, float]:
        area_extent_ll_list = list(map(lambda scn: scn.area.area_extent_ll, self.scenes))
        combined_area_extent = [0.0] * 4
        for area_extent in area_extent_ll_list:
            for index in range(4):
                cmp_func = min if index < 2 else max
                combined_area_extent[index] = cmp_func(area_extent[index], combined_area_extent[index])
        return tuple(combined_area_extent)

    @staticmethod
    def __to_scenes_ext(scenes: list[Scene] | list[SceneExt]) -> [SceneExt]:
        scenes_ext: list[SceneExt] = []
        if scenes is None:
            pass
        elif isinstance(scenes, Iterable):
            scn_list = list(scenes)
            if len(scn_list) != 0:
                if all(isinstance(scn, Scene) for scn in scn_list):
                    scenes_ext = list(map(lambda scn: SceneExt.from_scene(scn), scn_list))
                elif all(isinstance(scn, SceneExt) for scn in scn_list):
                    scenes_ext = scn_list
                else:
                    raise TypeError("scenes parameter is invalid")
        else:
            raise TypeError("scenes parameter must be a list of eigther Scene or SceneExt")
        return scenes_ext

    @staticmethod
    def __return_as_multiscene_ext_decorator(func) -> Callable[..., "MultiSceneExt"]:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            multi_scene = func(*args, **kwargs)
            return MultiSceneExt(multi_scene.scenes)
        return wrapper

    @staticmethod
    def from_multi_scene(multi_scene: MultiScene):
        multi_scene_ext = MultiSceneExt()
        old_vars = dict((key, value) for (key, value) in vars(multi_scene).items())
        for key, value in old_vars.items():
            setattr(multi_scene_ext, key, value)

    def copy(self, scenes: list[Scene] | list[SceneExt] = None) -> "MultiSceneExt":
        old_vars = dict((key, value) for (key, value) in vars(self).items())
        multi_scene_ext = MultiSceneExt(scenes)
        if scenes is not None:
            old_vars.pop("_scenes")
        for key, value in old_vars.items():
            setattr(multi_scene_ext, key, value)
        return multi_scene_ext

    def load(self, query, *args, **kwargs) -> None:
        super().load(query, *args, **kwargs)
        self.loaded.extend(query)

    def __get_group_by_wavelength(self, wavelength: float) -> tuple[DataQuery, list[str]]:
        filter_func = lambda data_id: wavelength in data_id["wavelength"]
        matching_data_ids = list(filter(filter_func, self.loaded_dataset_ids))

        wavelength_min = min((data_id["wavelength"].min for data_id in matching_data_ids))
        wavelength_max = max((data_id["wavelength"].max for data_id in matching_data_ids))
        wavelength_central = round((wavelength_max + wavelength_min) / 2, 2)

        resolution = min((data_id["resolution"] for data_id in matching_data_ids))
        matching_data_id_names = list(map(lambda data_id: data_id["name"], matching_data_ids))

        return (
            DataQuery(
                name=f"wavelength_{wavelength}",
                wavelength=(wavelength_min, wavelength_central, wavelength_max),
                resolution=resolution
            ),
            matching_data_id_names
        )

    def group_loaded(self):
        groups = dict(map(self.__get_group_by_wavelength, self.loaded))
        self.group(groups)

    def imshow_all_scenes(self, query: str | float):
        for scn in self.scenes:
            scn.imshow(query)

    def resample_all_to_eqc(self, *, resolution=None, **kwargs) -> "MultiScene":
        return MultiScene([scn.resample_to_eqc_area(resolution=resolution, **kwargs) for scn in self.scenes])

    def resample_all_to_eqc2(self, *, resolution=None, **kwargs) -> "MultiScene":
        projection = {"proj": "eqc"}  # Equidistant cylindrical projection
        new_area_args = {
            "area_id": "eqc_area",
            "projection": projection,
        }
        if resolution is not None:
            new_area_args["resolution"] = resolution
        new_area_def = create_area_def(**new_area_args)
        return self.resample(new_area_def, **kwargs)

    def get_pixel_size_in_degrees(self, band) -> float:
        return self.first_scene.get_pixel_size_in_degrees(band) if len(self.scenes) else 0.0

    def combine(self) -> SceneExt:
        self.group_loaded()
        multi_scn = self.resample_all_to_eqc(resolution=100000)
        combined_scn = multi_scn.blend(EqcMean.blend_func)
        combined_scn.load(self.loaded)
        return SceneExt.from_scene(combined_scn)
