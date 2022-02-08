from datetime import datetime
from satpy.writers import available_writers
import satpy
import matplotlib.pyplot as plt

import os
import sys
currDir = os.path.dirname(os.path.realpath(__file__))
rootDir = os.path.abspath(os.path.join(currDir, '..'))
if rootDir not in sys.path:
    sys.path.insert(0, rootDir)

from satellite.satellite_enum import SatelliteEnum
from satellite.satellite_collection import SatelliteCollection
from scene_handler.multiscene_ext import MultiSceneExt
from scene_handler.scene_ext import SceneExt


def main():
    satellite_enums = [
        SatelliteEnum.METOSAT8,
        SatelliteEnum.METOSAT11,
        SatelliteEnum.HIMAWARI8,
        SatelliteEnum.GOES16,
        SatelliteEnum.GOES17
    ]
    utctime = datetime(2022, 1, 12, 2, 29)
    # utctime = datetime(2022, 1, 12, 15, 29)
    query = 1.6
    # query = 10.3

    satellite_collection = SatelliteCollection(satellite_enums)
    file_readers = satellite_collection.download_all(utctime=utctime)

    scenes = list(map(lambda reader: reader.read_to_scene(), file_readers))
    multi_scn_ext = MultiSceneExt(scenes)

    multi_scn_ext.load([query])
    # scn_coll.imshow_all_scenes(query)

    comb_scene = multi_scn_ext.combine(resolution=50000)
    comb_scene.imshow(query)
    comb_scene.save_datasets(writer="geotiff")
    # comb_scene.save_datasets(writer="simple_image")


if __name__ == '__main__':
    main()
