import time
from datetime import datetime

from wwclouds.satellite.satellite_enum import SatelliteEnum
from wwclouds.satellite.satellite_mapping import SatelliteMapping
from wwclouds.satellite.satellite_type import SatelliteType
from wwclouds.satellite import downloader


class SatelliteCollection:
    def __init__(self, satellite_enums: [SatelliteEnum]):
        self.satellites: [SatelliteType] = list(map(SatelliteMapping.get_satellite_type, satellite_enums))

    def download_all(self, bands: [int] = None, utctime: datetime = datetime.utcnow()) -> [downloader.FileReader]:
        return [satellite.downloader.download(bands, utctime) for satellite in self.satellites]

    def download_all_most_recent(self, bands: [int] = None) -> [downloader.FileReader]:
        return self.download_all(bands)


if __name__ == '__main__':
    utctime = datetime(2022, 1, 12, 15, 29)
    start = time.time()
    # collection = Collection(SatelliteEnum.all())
    collection = SatelliteCollection([SatelliteEnum.METEOSAT8, SatelliteEnum.GOES16])
    file_readers = collection.download_all(utctime=utctime)
    print(time.time() - start)
    print(file_readers)

    import matplotlib.pyplot as plt
    from pyresample import create_area_def
    from satpy import MultiScene
    from satpy.composites import CloudCompositor
    from satpy.writers import to_image, get_enhanced_image

    my_area = create_area_def('my_area', {'a': '6378137', 'h': '35785863', 'lon_0': '140.7', 'no_defs': 'None', 'proj': 'geos', 'rf': '298.257024882273', 'type': 'crs', 'units': 'm', 'x_0': '0', 'y_0': '0'},
                              width=3000, height=3000,
                              units='degrees')

    my_band = "fog"

    for file_reader in file_readers:
        scn = file_reader.read_to_scene()
        print(scn.available_composite_names())
        scn.load([my_band])
        # new_scn = scn.resample()

        # img = get_enhanced_image(new_scn[my_band])

        plt.figure()
        # img.data.plot.imshow(vmin=0, vmax=1, rgb="bands")
        # plt.show()
        plt.figure()
        plt.imshow(scn[my_band])
        plt.show()
