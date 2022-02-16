import os
import sys
import warnings
from datetime import datetime
import xarray
from satpy.writers import available_writers
import satpy
import matplotlib.pyplot as plt
from satpy.writers import to_image
from satpy.composites import CloudCompositor
from time import time


currDir = os.path.dirname(os.path.realpath(__file__))
rootDir = os.path.abspath(os.path.join(currDir, '..'))
if rootDir not in sys.path:
    sys.path.insert(0, rootDir)

if not sys.warnoptions:
    warnings.simplefilter("ignore")


from satellite.satellite_enum import SatelliteEnum
from satellite.satellite_collection import SatelliteCollection
from scene_handler.multiscene_ext import MultiSceneExt
from scene_handler.scene_ext import SceneExt


def main():
    satellite_enums = [
        SatelliteEnum.METEOSAT8,
        SatelliteEnum.METEOSAT11,
        SatelliteEnum.HIMAWARI8,
        SatelliteEnum.GOES16,
        SatelliteEnum.GOES17
    ]
    # utctime = datetime(2022, 1, 12, 2, 29)
    # utctime = datetime(2022, 1, 12, 15, 29)
    utctime = datetime(2022, 2, 11, 9, 29)
    # query = 1.6
    query = [10.3, 12.3]

    start_time = time()
    satellite_collection = SatelliteCollection(satellite_enums)
    file_readers = satellite_collection.download_all(utctime=utctime)
    print(f"downloaded: {time() - start_time}")

    scenes = list(map(lambda reader: reader.read_to_scene(), file_readers))
    multi_scn_ext = MultiSceneExt(scenes)

    multi_scn_ext.load(query)
    print(f"loaded mscn: {time() - start_time}")

    comb_scene = multi_scn_ext.combine(resolution=20000)
    print(f"combined: {time() - start_time}")

    comb_scene.save_datasets(writer="geotiff")
    # comb_scene.save_datasets(writer="simple_image")
    compositor = CloudCompositor(
        "clouds",
        # 269.0,
        # 270.0
    )
    composite = compositor([comb_scene[band] for band in query])
    for band in query:
        comb_scene.imshow(band)
    img = to_image(composite)
    img.save("clouds5.tif")
    print(f"total: {time() - start_time}")


if __name__ == '__main__':
    main()
