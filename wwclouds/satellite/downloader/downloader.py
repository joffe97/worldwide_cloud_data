from datetime import datetime, timedelta
import os
import abc
from typing import Union, List
import time as t
from glob import glob
from pathlib import Path

import wwclouds
import wwclouds.config as config
from .file_reader import FileReader


class Downloader(metaclass=abc.ABCMeta):
    data_dir = f"{wwclouds.ROOT_DIR}/{config.DATA_PATH}"

    def __init__(self,
                 subdir: str,
                 reader: str,
                 update_frequency: timedelta,
                 all_bands: Union[List[int], List[str], None] = None
                 ):
        self.path = f"{self.data_dir}/{subdir}"
        self.reader = reader
        self.update_frequency = update_frequency
        self.all_bands = all_bands

    @abc.abstractmethod
    def _download(self, bands: Union[List[int], None], time: datetime) -> [str]:
        pass

    def create_dir_if_not_exist(self):
        if not os.path.exists(self.path):
            os.makedirs(self.path)

    def get_local_file_path(self, file_path: str) -> str:
        local_file_name = file_path.split("/")[-1].split("=")[-1]
        return f"{self.path}/{local_file_name}"

    def __get_local_file_path_without_file_ending(self, external_path: str) -> str:
        local_file_path = self.get_local_file_path(external_path)
        parts = local_file_path.split("/")
        stripped_ending = parts[-1].split(".")[0]
        return "/".join([*parts[:-1], stripped_ending])

    def file_is_downloaded(self, external_path: str):
        return bool(glob(f"{self.__get_local_file_path_without_file_ending(external_path)}.*"))

    def get_previous_update_time(self, time: datetime) -> datetime:
        update_frequency_seconds = self.update_frequency.seconds // 60
        last_update_minute = (time.minute // update_frequency_seconds) * update_frequency_seconds
        return datetime(time.year, time.month, time.day, time.hour, last_update_minute)

    def download(self, bands: Union[List[int], None] = None, time: datetime = datetime.utcnow()) -> FileReader:
        if bands is None:
            bands = self.all_bands

        previous_updated_time = self.get_previous_update_time(time)
        self.create_dir_if_not_exist()

        start = t.time()
        file_paths = self._download(bands, previous_updated_time)
        file_reader = FileReader(file_paths, reader=self.reader)
        print(t.time() - start)
        return file_reader
