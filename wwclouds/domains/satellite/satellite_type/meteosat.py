from wwclouds.domains.satellite.satellite_type import SatelliteType
from wwclouds.domains.satellite import downloader
from wwclouds.domains.satellite.satellite_enum import SatelliteEnum


class Meteosat(SatelliteType):
    def __init__(self, satellite_enum: SatelliteEnum):
        super().__init__(downloader.Meteosat(satellite_enum), dict())
