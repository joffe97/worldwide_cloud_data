import time

from satpy import Scene, MultiScene, DataQuery, DataID
from satpy.dataset.dataid import WavelengthRange
from pyresample import create_area_def, AreaDefinition
from typing import Callable, Union
import functools
from collections.abc import Iterable
from xarray import DataArray
from wwclouds.scene_handler.scene_ext import SceneExt
from wwclouds.scene_handler.eqc_blend import EqcBlend


class MultiSceneExt(MultiScene):
    def __init__(self, scenes: Union[list[Scene], list[SceneExt]] = None):
        super().__init__(self.__to_scenes_ext(scenes))
        self.loaded: list[Union[str, float, int]] = []

        self.__shared_dataset_ids_override: Union[set[DataID], None] = None

    @property
    def shared_dataset_ids(self) -> set[DataID]:
        return self.__shared_dataset_ids_override if self.__shared_dataset_ids_override is not None \
            else super().shared_dataset_ids

    @shared_dataset_ids.setter
    def shared_dataset_ids(self, value):
        self.__shared_dataset_ids_override = value

    def remove_override_for_shared_dataset_ids(self) -> None:
        self.__shared_dataset_ids_override = None

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
    def __to_scenes_ext(scenes: Union[list[Scene], list[SceneExt]]) -> [SceneExt]:
        scenes_ext: list[SceneExt] = []
        if scenes is None:
            pass
        elif isinstance(scenes, Iterable):
            scn_list = list(scenes)
            if len(scn_list) != 0:
                if all(isinstance(scn, SceneExt) for scn in scn_list):
                    scenes_ext = scn_list
                elif all(isinstance(scn, Scene) for scn in scn_list):
                    scenes_ext = list(map(lambda scn: SceneExt.from_scene(scn), scn_list))
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

    def copy(self, override_scenes: Union[list[Scene], list[SceneExt]] = None) -> "MultiSceneExt":
        old_vars = dict((key, value) for (key, value) in vars(self).items())
        multi_scene_ext = MultiSceneExt(override_scenes)
        if override_scenes is not None:
            old_vars.pop("_scenes")
            old_vars.pop("_scene_gen")
        for key, value in old_vars.items():
            setattr(multi_scene_ext, key, value)
        return multi_scene_ext

    def load(self, query, *args, **kwargs) -> None:
        super().load(query, *args, **kwargs)
        self.loaded.extend(query)

    def unload(self, keepables: Iterable):
        for scene in self.scenes:
            scene.unload(keepables)
        for index, band in enumerate(self.loaded):
            if band not in keepables:
                self.loaded.pop(index)

    def __get_group_by_wavelength(self, wavelength: float) -> Union[tuple[DataQuery, list[str]], None]:
        filter_func = lambda data_id: wavelength in data_id["wavelength"]
        matching_data_ids = list(filter(filter_func, self.loaded_dataset_ids))

        if len(matching_data_ids) == 0:
            return None

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

    def group_loaded(self) -> set[DataID]:
        group_tuples = map(self.__get_group_by_wavelength, self.loaded)
        legal_groups = dict(filter(lambda tup: tup is not None, group_tuples))
        prev_shared_datasets = self.shared_dataset_ids
        self.group(legal_groups)
        return self.shared_dataset_ids - prev_shared_datasets

    def imshow_all_scenes(self, query: Union[str, float]):
        for scn in self.scenes:
            scn.imshow(query)

    def resample(self, destination=None, **kwargs) -> "MultiSceneExt":
        new_multi_scn = super().resample(destination, **kwargs)
        return self.copy(new_multi_scn.scenes)

    def resample_all_to_eqc(self, resolution=None, **kwargs) -> "MultiSceneExt":
        return MultiSceneExt([
            scn.resample_to_eqc_area(resolution=resolution, reduce_data=False, **kwargs) for scn in self.scenes
        ])

    def resample_loaded_to_eqc(self, resolution=None, **kwargs):
        start_time = time.time()
        groups = self.group_loaded()
        eqc_mscn = self.resample_all_to_eqc(resolution, **kwargs)
        eqc_mscn.shared_dataset_ids = groups
        print(f"Resampled scenes: {round(time.time() - start_time, 4)} sec")
        return eqc_mscn

    def combine(self, max_latitude) -> SceneExt:
        if len(self.scenes) == 0:
            raise ValueError("cannot combine MultiSceneExt with 0 scenes")
        start_time = time.time()
        eqc_blend = EqcBlend((-max_latitude, max_latitude))
        combined_scn = self.blend(eqc_blend)
        print(f"Combined scenes: {round(time.time() - start_time, 4)} sec")
        combined_scn_ext = SceneExt.from_scene(combined_scn)
        combined_scn_ext.load(self.loaded)
        return combined_scn_ext
