from wwclouds.scene_handler.multiscene_ext import MultiSceneExt
from wwclouds.scene_handler.earth_map import EarthMap


class Combiner:
    @staticmethod
    def combine(scene_collection: MultiSceneExt):
        for band in scene_collection.loaded:
            pixel_size_deg = scene_collection.get_pixel_size_in_degrees(band)
            earth_map = EarthMap(pixel_size_deg, band)
            for scene_ext in scene_collection.scenes_ext:
                print(scene_ext.scene[band])
                exit()
                earth_map.add_values_from_scene_ext(scene_ext)
            earth_map.imshow()
