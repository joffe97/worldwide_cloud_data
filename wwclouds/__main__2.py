import os
import sys
import warnings
from datetime import datetime

import matplotlib.pyplot
import xarray
from satpy.writers import available_writers, get_enhanced_image
from satpy.composites import GenericCompositor
import satpy
import matplotlib.pyplot as plt
from satpy.writers import to_image
from satpy.composites import CloudCompositor, DifferenceCompositor, DayNightCompositor
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
    # query = [3.8, 10.3, 12.3]
    query = [
        # 0.65,
        # 0.86,
        # 1.6,
        # 3.8,
        # 10.3,
        12.3,
    ]

    start_time = time()
    satellite_collection = SatelliteCollection(satellite_enums)
    file_readers = satellite_collection.download_all(utctime=utctime)
    print(f"downloaded: {time() - start_time}")

    scenes = list(map(lambda reader: reader.read_to_scene(), file_readers))
    multi_scn_ext = MultiSceneExt(scenes)

    for scn in scenes:
        print(scn.available_dataset_ids())
    multi_scn_ext.load(query, resolution=[500, 1000, 2000, 3000.403165817])
    # multi_scn_ext.load(["VIS008", ""])
    print(f"loaded mscn: {time() - start_time}")

    comb_scene = multi_scn_ext.combine(resolution=60000)
    # comb_scene = multi_scn_ext.scenes[0]
    print(f"combined: {time() - start_time}")

    for band in query:
        comb_scene.imshow(band)
    # new_scn = (comb_scene[0.65] - comb_scene[1.6]) / (comb_scene[0.65] + comb_scene[1.6])
    # combined_bands = (comb_scene[10.3] - comb_scene[12.3])
    # combined_bands = (comb_scene[10.3] - comb_scene[3.8])
    # comp = GenericCompositor("my_test")

    # cloud_comp = CloudCompositor("cloud_test", 300)
    # diff_comp = DifferenceCompositor("diff_test")
    # comp = GenericCompositor("generic_comp")
    # # my_test = comp((combined_bands,))
    # # my_test = diff_comp((comb_scene[10.3], comb_scene[3.8]))
    # # my_test = cloud_comp((my_test))
    # my_test = comp((diff_comp((comb_scene[12.3], comb_scene[10.3])), diff_comp((comb_scene[10.3], comb_scene[3.8])), comb_scene[10.3]))
    # img = to_image(my_test)
    # img.stretch("linear")
    # img.gamma(1.7)
    # img.save("testing2.tif")
    # print(combined_bands)
    # exit()

    comb_scene.save_datasets(writer="geotiff")
    print(comb_scene.area.proj_str)
    exit()
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
    img.save("clouds.png")
    img.save("clouds.tif")
    print(f"total: {time() - start_time}")


if __name__ == '__main__':
    main()
