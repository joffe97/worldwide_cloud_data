from enum import Enum, auto
from datetime import datetime, timedelta
from typing import Union
from functools import lru_cache
import re

from wwclouds.satellite.downloader.aws import Aws
from wwclouds.satellite.satellite_enum import SatelliteEnum


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
            update_frequency=timedelta(minutes=10),
            all_bands=list(map(str, range(1, 17))))

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

    def _get_previous_keys_for_band(self, band: str, time: datetime, retries: int = 3) -> [str]:
        if retries == 0:
            return []
        file_entries = [file_entry for file_entry in self._get_all_object_keys_for_band_in_aws_directory(band, time)]
        start_time = None
        prev_file_entry = None
        for file_entry in file_entries:
            start_time_tmp = self._get_scan_start_time_from_object_key(file_entry)
            if start_time_tmp > time:
                continue
            elif prev_file_entry is None or start_time < start_time_tmp:
                prev_file_entry = file_entry
                start_time = start_time_tmp
        if None in [prev_file_entry, start_time] or start_time > time:
            return self._get_previous_keys_for_band(band, time - self.update_frequency, retries - 1)
        return [prev_file_entry]

    def _get_scan_start_time_from_object_key(self, object_key: str) -> datetime:
        start_time_regex = re.compile(r"^.*_s(\d+)._.*$")
        start_time_str = start_time_regex.match(object_key).groups()[0]
        return datetime.strptime(start_time_str, "%Y%j%H%M%S")


if __name__ == "__main__":
    satellite_enum = SatelliteEnum.GOES16
    noaa_goes = NoaaGoes(satellite_enum)
    file_reader = noaa_goes.download(time=datetime(2022, 1, 12, 15, 29))
    print(file_reader.filepaths)
    scn = file_reader.read_to_scene()
    print(scn.available_dataset_names())
    my_scene = "C06"
    scn.load([my_scene])

    import matplotlib.pyplot as plt
    plt.figure()
    plt.imshow(scn[my_scene])
    plt.show()
