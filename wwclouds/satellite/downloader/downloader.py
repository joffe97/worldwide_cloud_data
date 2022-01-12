from datetime import datetime, timedelta
import os
import abc
from typing import Union, List

import wwclouds
import wwclouds.config as config
from .file_reader import FileReader


class Downloader(metaclass=abc.ABCMeta):
    data_dir = f"{wwclouds.ROOT_DIR}/{config.DATA_PATH}"

    def __init__(self, subdir: str, reader: str, update_frequency: timedelta):
        self.path = f"{self.data_dir}/{subdir}"
        self.reader = reader
        self.update_frequency = update_frequency

    def create_dir_if_not_exist(self):
        if not os.path.exists(self.path):
            os.makedirs(self.path)

    def get_local_file_path(self, key: str) -> str:
        file_name = key.split("/")[-1].split("=")[-1]
        return f"{self.path}/{file_name}"

    def get_previous_update_time(self, time: datetime) -> datetime:
        update_frequency_seconds = self.update_frequency.seconds // 60
        last_update_minute = (time.minute // update_frequency_seconds) * update_frequency_seconds
        return datetime(time.year, time.month, time.day, time.hour, last_update_minute)

    @abc.abstractmethod
    def download(self, bands: Union[List[int], None] = None, time: datetime = datetime.utcnow()) -> FileReader:
        pass
