import boto3
import boto3.s3.transfer as s3transfer
from botocore import UNSIGNED
from botocore.config import Config
from datetime import datetime, timedelta
from typing import Iterator, Union, List
import os
import time as t
import abc

from wwclouds.satellite.downloader.downloader import Downloader
from wwclouds.satellite.downloader.file_reader import FileReader


class FileEntry:
    def __init__(self, key: str, time: datetime):
        self.key = key
        self.time = time


class Aws(Downloader, metaclass=abc.ABCMeta):
    s3_client = boto3.client("s3", config=Config(signature_version=UNSIGNED, max_pool_connections=20))
    transfer_config = s3transfer.TransferConfig(max_concurrency=20, use_threads=True)
    s3t = s3transfer.create_transfer_manager(s3_client, transfer_config)

    def __init__(self, bucket: str, product: str, reader: str, update_frequency: timedelta, all_bands: [int]):
        super().__init__(subdir=f"{bucket}/{product}", reader=reader, update_frequency=update_frequency)
        self.bucket = bucket
        self.product = product
        self.all_bands = all_bands

    @abc.abstractmethod
    def _get_aws_prefix_for_band(self, band: int, time: datetime) -> str:
        pass

    @abc.abstractmethod
    def _get_latest_keys_for_band(self, band: int, time: datetime, retries: int = 3) -> [str]:
        pass

    def _file_posthandler(self, filepath: str) -> str:
        return filepath

    def _get_all_file_entries_for_band_in_aws_directory(self, band: int, time: datetime) -> Iterator[FileEntry]:
        prefix = self._get_aws_prefix_for_band(band, time)
        kwargs = {"Bucket": self.bucket, "Prefix": prefix}
        while True:
            response = self.s3_client.list_objects_v2(**kwargs)
            if "Contents" not in response:
                response["Contents"] = []
            for obj in response["Contents"]:
                key = obj["Key"]
                if not key.startswith(prefix):
                    continue
                time = obj["LastModified"]
                yield FileEntry(key, time)
            try:
                kwargs["ContinuationToken"] = response["NextContinuationToken"]
            except KeyError:
                break

    def _get_latest_keys(self, bands: [int], time: datetime) -> [[str]]:
        return [self._get_latest_keys_for_band(band, time) for band in bands]

    def _download(self, bands: [int], time: datetime) -> [str]:
        self.create_dir_if_not_exist()
        keys_list = self._get_latest_keys(bands, time)
        file_paths = []
        for keys in keys_list:
            for key in keys:
                file_path = self.get_local_file_path(key)
                if not os.path.exists(file_path):
                    self.s3t.download(self.bucket, key, file_path)
                file_paths.append(file_path)
        self.s3t.shutdown()
        return list(map(self._file_posthandler, file_paths))

    # Testing band 7 downloads
    # Himawari download - Concurrent: 16.9 sec. Non-concurrent: 82.1 sec.
    #                                 19.5                      79.2
    #                                 17.0                      80.4
    # Goes-17 download  - Concurrent: 33.3 sec. Non-concurrent: 37.7 sec.
    #                                 47.0                      38.7
    #                                 40.7                      30.4
    def download(self, bands: Union[List[int], None] = None, time: datetime = datetime.utcnow()) -> FileReader:
        if bands is None:
            bands = self.all_bands
        start = t.time()
        file_reader = FileReader(self._download(bands, time), reader=self.reader)
        print(t.time() - start)
        return file_reader


if __name__ == "__main__":
    aws = Aws("noaa-goes17", "ABI-L1b-RadF", "abi_l1b", timedelta(minutes=10), list(range(1, 17)))
    file_reader = aws.download([6, 7, 8], datetime.utcnow())
    scn = file_reader.read_to_scene()
    my_scene = "C07"
    scn.load([my_scene])
    import matplotlib.pyplot as plt

    plt.figure()
    plt.imshow(scn[my_scene])
    plt.show()
