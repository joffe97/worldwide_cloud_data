import abc
from typing import Optional
from wwclouds.domains.satellite.downloader import Downloader


class SatelliteType(metaclass=abc.ABCMeta):
    def __init__(self, downloader: Downloader, band_frequency_mapping: dict[str, tuple[float, float]]):
        self.downloader = downloader
        self.__band_frequency_mapping = band_frequency_mapping

    def get_band_for_frequency(self, frequency: float) -> Optional[str]:
        for band, frequencies in self.__band_frequency_mapping.items():
            f_min, f_max = sorted(frequencies)
            if f_min <= frequency <= f_max:
                return band
        return None

    def get_band_for_frequencies(self, frequencies: list[float]) -> list[str]:
        return list(map(self.get_band_for_frequency, frequencies))
