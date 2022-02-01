import abc
from wwclouds.satellite.downloader import Downloader


class SatelliteType(metaclass=abc.ABCMeta):
    def __init__(self, downloader: Downloader = None):
        self.downloader = downloader
