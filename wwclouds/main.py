from datetime import datetime

from satellite.satellite_enum import SatelliteEnum
from satellite.satellite_collection import SatelliteCollection
from scene_handler.multiscene_ext import MultiSceneExt
from scene_handler.scene_ext import SceneExt


def main():
    satellite_enums = [
        # SatelliteEnum.METOSAT8,
        # SatelliteEnum.METOSAT11,
        # SatelliteEnum.HIMAWARI8,
        SatelliteEnum.GOES16,
        SatelliteEnum.GOES17
    ]
    # utctime = datetime(2022, 1, 12, 2, 29)
    utctime = datetime(2022, 1, 12, 15, 29)
    query = 1.6
    # query = 10.3

    satellite_collection = SatelliteCollection(satellite_enums)
    file_readers = satellite_collection.download_all(utctime=utctime)

    scenes = list(map(lambda reader: reader.read_to_scene(), file_readers))
    scene_collection = MultiSceneExt(scenes)

    scene_collection.load([query])
    # scn_coll.imshow_all_scenes(query)

    comb_scene = scene_collection.combine()
    comb_scene_ext = SceneExt.from_scene(comb_scene)
    comb_scene_ext.imshow(query)


if __name__ == '__main__':
    main()
