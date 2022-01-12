from wwclouds.satellite.downloader import Downloader
import abc


class SatelliteType(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def __init__(self, downloader: Downloader = None, **kwargs):
        self.downloader = downloader
