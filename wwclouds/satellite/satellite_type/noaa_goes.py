from wwclouds.satellite.satellite_type import SatelliteType
from wwclouds.satellite import downloader
from wwclouds.satellite.satellite_enum import SatelliteEnum


class NoaaGoes(SatelliteType):
    def __init__(self, satellite_enum: SatelliteEnum):
        super().__init__(downloader.NoaaGoes(satellite_enum))
