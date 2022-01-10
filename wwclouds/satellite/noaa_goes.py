from enum import Enum
from goes2go.data import goes_latest
import os
import pandas as pd

from .satellite import Satellite
from wwclouds.satellite.downloader.file_reader import FileReader


class NoaaGoesType(Enum):
    GOES16 = 1
    GOES17 = 2

    def as_str(self):
        return self.name


class NoaaGoes(Satellite):
    def __init__(self, noaa_goes_type: NoaaGoesType):
        self.type = noaa_goes_type
        super().__init__()
        
    def download_data(self):
        data_dir = "./data"
        os.makedirs(data_dir, exist_ok=True)
        filelist_dataframe: pd.DataFrame = goes_latest(satellite=self.type.as_str(), product="ABI", domain="FULL", return_as="filelist", save_dir=data_dir)
        filelist = list(map(lambda file_ending: f"{data_dir}/{file_ending}", filelist_dataframe["file"]))
        self.file_reader = FileReader(filelist, "abi_l2_nc")
