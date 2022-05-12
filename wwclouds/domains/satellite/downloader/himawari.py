import os.path
from datetime import datetime, timedelta
import bz2
import re

from wwclouds.domains.satellite.downloader.aws import Aws


class Himawari(Aws):
    def __init__(self):
        super().__init__(
            bucket="noaa-himawari8",
            product="AHI-L1b-FLDK",
            reader="ahi_hsd",
            update_frequency=timedelta(minutes=10))

    def __get_aws_directory(self, update_time: datetime) -> str:
        return f"{self.product}/{update_time.year}/{update_time.month:02.0f}/{update_time.day:02.0f}" \
               f"/{update_time.hour:02.0f}{update_time.minute:02.0f}"

    def _get_aws_prefix_for_band(self, band: str, update_time: datetime) -> str:
        prev_update_time = self._get_previous_update_time(update_time)
        return f"{self.__get_aws_directory(prev_update_time)}/HS_H08_{prev_update_time.year}{prev_update_time.month:02.0f}" \
               f"{prev_update_time.day:02.0f}_{prev_update_time.hour:02.0f}{prev_update_time.minute:02.0f}_B{band}_FLDK"

    def _file_posthandler(self, filepath: str) -> str:
        new_filepath = filepath.rstrip(".bz2")
        if not os.path.exists(new_filepath):
            if not os.path.exists(filepath):
                raise FileNotFoundError("bz2 encrypted file cannot be found")
            with open(new_filepath, "wb") as new_file, bz2.BZ2File(filepath, "rb") as file:
                for data in iter(lambda: file.read(100 * 1024), b""):
                    new_file.write(data)
            os.remove(filepath)
        return new_filepath

    def _get_scan_start_time_from_object_key(self, object_key: str) -> datetime:
        start_time_regex = re.compile(r"^.*_(\d{8}_\d{4})_.*$")
        start_time_str = start_time_regex.match(object_key).groups()[0]
        return datetime.strptime(start_time_str, "%Y%m%d_%H%M")
