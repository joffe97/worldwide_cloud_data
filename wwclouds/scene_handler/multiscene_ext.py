from satpy import Scene, MultiScene, DataQuery
from pyresample import create_area_def, AreaDefinition
from typing import Callable
import functools
from collections.abc import Iterable
from wwclouds.scene_handler.scene_ext import SceneExt


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

    def copy(self, scenes: list[Scene] | list[SceneExt] = None) -> "MultiSceneExt":
        multi_scene_ext = MultiSceneExt()
        old_vars = dict((key, value) for (key, value) in vars(self).items())
        if scenes is not None:
            old_vars["_scenes"] = scenes
        for key, value in old_vars.items():
            setattr(multi_scene_ext, key, value)
        return multi_scene_ext

    def load(self, query, *args, **kwargs) -> None:
        super().load(query, *args, **kwargs)
        self.loaded.extend(query)

    def group_loaded(self):
        groups = dict()
        for band in self.loaded:
            groups[DataQuery(name="my_band", wavelength=(band - 0.2, band, band + 0.2), resolution=200000)] \
                = ["IR_016", "C05", "B05"]
        self.group(groups)

    # def resample(self, *args, **kwargs) -> "MultiSceneExt":
        # scenes = super().resample(*args, **kwargs).scenes
        # return self.copy(scenes)

    def imshow_all_scenes(self, query: str | float):
        for scn in self.scenes:
            scn.imshow(query)

    def resample_all_to_eqc(self, **kwargs) -> "MultiSceneExt":
        return self.copy(list(map(lambda scn: scn.resample_to_eqc_area(resolution=20, **kwargs), self.scenes)))

    @__return_as_multiscene_ext_decorator
    def resample_all_to_eqc2(self, **kwargs) -> "MultiSceneExt":
        projection = {"proj": "eqc"}  # Equidistant cylindrical projection
        new_area_args = {
            "area_id": "eqc_area",
            "projection": projection,
            "resolution": 20,
            "radius": [180, 90],
            "units": "degrees"
        }
        new_area_def = create_area_def(**new_area_args)
        return self.resample(new_area_def, **kwargs)

    def get_pixel_size_in_degrees(self, band) -> float:
        return self.first_scene.get_pixel_size_in_degrees(band) if len(self.scenes) else 0.0

    def combine(self) -> Scene:
        self.group_loaded()
        for scn in self.scenes:
            print(scn.available_dataset_names())
            print(scn[1.6].attrs["area"])
            print()
        multi_scn = self.resample_all_to_eqc()
        multi_scn = multi_scn.resample()
        print(multi_scn.shared_dataset_ids)
        for scn in multi_scn.scenes:
            print(scn.available_dataset_names())
            print(scn[1.6].attrs["area"])
            print(scn.get_lonlats(1.6))
            print()
        combined_scn = multi_scn.blend()
        combined_scn.load([1.6])
        return combined_scn
