from wwclouds.satellite.satellite_type import SatelliteType
from wwclouds.satellite import downloader


class Himawari(SatelliteType):
    def __init__(self, **kwargs):
        super().__init__(downloader.Himawari())
