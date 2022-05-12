from enum import Enum, auto
from datetime import datetime, timedelta
from typing import Union
from functools import lru_cache
import re

from wwclouds.domains.satellite.downloader.aws import Aws
from wwclouds.domains.satellite.satellite_enum import SatelliteEnum


class NoaaGoesType(Enum):
    GOES16 = auto()
    GOES17 = auto()

    @property
    def bucket_name(self):
        return f"noaa-{self.name.lower()}"

    @staticmethod
    def from_str(string: str) -> "NoaaGoesType":
        return getattr(NoaaGoesType, string)

    @staticmethod
    def from_satellite_flag(satellite_enum: SatelliteEnum) -> "NoaaGoesType":
        return NoaaGoesType.from_str(satellite_enum.name)


class NoaaGoes(Aws):
    def __init__(self, satellite_enum: SatelliteEnum):
        noaa_goes_type = NoaaGoesType.from_satellite_flag(satellite_enum)
        super().__init__(
            bucket=noaa_goes_type.bucket_name,
            product="ABI-L1b-RadF",
            reader="abi_l1b",
            update_frequency=timedelta(minutes=10))

    def __get_aws_directory(self, time: datetime) -> str:
        day_of_year = time.timetuple().tm_yday
        return f"{self.product}/{time.year}/{day_of_year:03.0f}/{time.hour:02.0f}"

    @lru_cache(maxsize=32)
    def __get_scan_mode(self, time: datetime) -> Union[int, None]:
        prefix = f"{self.__get_aws_directory(time)}/OR_{self.product}-M"
        scan_mode_index = len(prefix)
        for obj in self._iter_aws_by_prefix(prefix):
            return int(obj["Key"][scan_mode_index])
        return None

    def _get_aws_prefix_for_band(self, band: str, time: datetime) -> str:
        return f"{self.__get_aws_directory(time)}/OR_{self.product}-M{self.__get_scan_mode(time)}C{band}"

    def _get_scan_start_time_from_object_key(self, object_key: str) -> datetime:
        start_time_regex = re.compile(r"^.*_s(\d+)._.*$")
        start_time_str = start_time_regex.match(object_key).groups()[0]
        return datetime.strptime(start_time_str, "%Y%j%H%M%S")
