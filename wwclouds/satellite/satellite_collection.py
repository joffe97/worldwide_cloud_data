from datetime import datetime
from typing import Optional
import functools

from wwclouds.satellite.satellite_enum import SatelliteEnum
from wwclouds.satellite.satellite_mapping import SatelliteMapping
from wwclouds.satellite.satellite_type import SatelliteType
from wwclouds.satellite import downloader


class SatelliteCollection:
    def __init__(self, satellite_enums: [SatelliteEnum]):
        self.satellites: [SatelliteType] = list(map(SatelliteMapping.get_satellite_type, satellite_enums))

    def get_scan_start_times(self, frequencies: list[float], utctime: datetime) -> list[datetime]:
        scan_start_times = []
        for satellite in self.satellites:
            bands = satellite.get_band_for_frequencies(frequencies)
            scan_start_time = satellite.downloader.get_first_scan_start_time_for_bands(bands, utctime)
            scan_start_times.append(scan_start_time)
        return sorted(scan_start_times)

    def get_scan_times_strings(self, frequencies: list[float], utctime: datetime) -> tuple[str, str]:
        scan_times = self.get_scan_start_times(frequencies, utctime)
        day_str = scan_times[0].strftime("%y%m%d").zfill(6)
        times_str_list = list(
            str(scan_time.hour * 3600 + scan_time.minute * 60 + scan_time.second).zfill(5) for scan_time in scan_times
        )
        return day_str, ''.join(times_str_list)

    def download_all(self, frequencies: Optional[list[float]], utctime: datetime) -> [downloader.FileReader]:
        if frequencies is None:
            frequencies = []
        file_readers = []
        for satellite in self.satellites:
            bands = satellite.get_band_for_frequencies(frequencies)
            file_readers.append(satellite.downloader.download(bands, utctime))
        return file_readers


if __name__ == '__main__':
    pass
