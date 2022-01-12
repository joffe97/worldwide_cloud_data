import time
from datetime import datetime

from wwclouds.satellite.satellite_enum import SatelliteEnum
from wwclouds.satellite.satellite_mapping import SatelliteMapping
from wwclouds.satellite.satellite_type import SatelliteType
from wwclouds.satellite import downloader


class Collection:
    def __init__(self, satellite_enums: [SatelliteEnum]):
        self.satellites: [SatelliteType] = list(map(SatelliteMapping.get_satellite_type, satellite_enums))

    def download_all(self, bands: [int], utctime: datetime) -> [downloader.FileReader]:
        return [satellite.downloader.download(bands, utctime) for satellite in self.satellites]

    def download_all_most_recent(self, bands: [int]) -> [downloader.FileReader]:
        return self.download_all(bands, datetime.utcnow())


if __name__ == '__main__':
    start = time.time()
    collection = Collection(SatelliteEnum.all())
    file_readers = collection.download_all([5], datetime(2022, 1, 12, 15, 29))
    print(time.time() - start)
