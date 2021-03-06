from datetime import datetime, timedelta
import os
import abc
from typing import List, Optional
import time as t
from glob import glob

import wwclouds.config as config
from .file_reader import FileReader


class Downloader(metaclass=abc.ABCMeta):
    def __init__(self,
                 subdir: str,
                 reader: str,
                 update_frequency: timedelta):
        self.subdir = subdir
        self.reader = reader
        self.update_frequency = update_frequency

    @abc.abstractmethod
    def _download(self, bands: Optional[List[str]], time: datetime) -> [str]:
        pass

    @abc.abstractmethod
    def _get_previous_scan_start_time_for_band(self, band: str, time: datetime) -> datetime:
        pass

    @property
    def __path(self):
        return f"{config.DATA_PATH_DOWNLOADS}/{self.subdir}"

    def __create_dir_if_not_exist(self):
        if not os.path.exists(self.__path):
            os.makedirs(self.__path)

    def _get_local_file_path(self, file_path: str) -> str:
        local_file_name = file_path.split("/")[-1].split("=")[-1]
        return f"{self.__path}/{local_file_name}"

    def __get_local_file_path_without_file_ending(self, external_path: str) -> str:
        local_file_path = self._get_local_file_path(external_path)
        parts = local_file_path.split("/")
        stripped_ending = parts[-1].split(".")[0]
        return "/".join([*parts[:-1], stripped_ending])

    def _file_is_downloaded(self, external_path: str):
        return bool(glob(f"{self.__get_local_file_path_without_file_ending(external_path)}.*"))

    def _get_previous_update_time(self, time: datetime) -> datetime:
        update_frequency_seconds = self.update_frequency.seconds // 60
        last_update_minute = (time.minute // update_frequency_seconds) * update_frequency_seconds
        return datetime(time.year, time.month, time.day, time.hour, last_update_minute)

    def get_first_scan_start_time_for_bands(self, bands: list[str], time: datetime) -> datetime:
        scan_start_times = [self._get_previous_scan_start_time_for_band(band, time) for band in bands]
        return min(scan_start_times)

    def download(self, bands: Optional[List[Optional[str]]] = None, time: datetime = datetime.utcnow()) -> FileReader:
        if None in bands:
            bands = None
        self.__create_dir_if_not_exist()
        start = t.time()
        file_paths = self._download(bands, time)
        file_reader = FileReader(file_paths, reader=self.reader)
        print(f"Downloaded {self.subdir}: {round(t.time() - start, 4)} sec")
        return file_reader
