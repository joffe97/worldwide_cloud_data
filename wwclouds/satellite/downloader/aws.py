import boto3
import boto3.s3.transfer as s3transfer
from botocore import UNSIGNED
from botocore.config import Config
from datetime import datetime, timedelta
from typing import Iterator
import abc
import functools

from wwclouds.satellite.downloader.downloader import Downloader


class Aws(Downloader, metaclass=abc.ABCMeta):
    s3_client = boto3.client("s3", config=Config(signature_version=UNSIGNED, max_pool_connections=10))
    transfer_config = s3transfer.TransferConfig(max_concurrency=10, use_threads=True)

    def __init__(self, bucket: str, product: str, reader: str, update_frequency: timedelta, all_bands: [int]):
        super().__init__(
            subdir=f"{bucket}/{product}",
            reader=reader,
            update_frequency=update_frequency,
            all_bands=all_bands
        )
        self.bucket = bucket
        self.product = product

    @abc.abstractmethod
    def _get_aws_prefix_for_band(self, band: str, time: datetime) -> str:
        pass

    # @abc.abstractmethod
    # def _get_previous_keys_for_band(self, band: str, time: datetime, retries: int = 3) -> [str]:
        # pass

    @abc.abstractmethod
    def _get_scan_start_time_from_object_key(self, object_key: str) -> datetime:
        pass

    def _iter_aws_by_prefix(self, prefix) -> Iterator[dict[str, any]]:
        kwargs = {"Bucket": self.bucket, "Prefix": prefix}
        while True:
            response = self.s3_client.list_objects_v2(**kwargs)
            if "Contents" not in response:
                response["Contents"] = []
            for obj in response["Contents"]:
                key = obj["Key"]
                if not key.startswith(prefix):
                    continue
                yield obj
            try:
                kwargs["ContinuationToken"] = response["NextContinuationToken"]
            except KeyError:
                break

    def _file_posthandler(self, filepath: str) -> str:
        return filepath

    def _get_all_object_keys_for_band_in_aws_directory(self, band: str, time: datetime) -> str:
        prefix = self._get_aws_prefix_for_band(band, time)
        for obj in self._iter_aws_by_prefix(prefix):
            yield obj["Key"]

    @functools.lru_cache(32)
    def _get_previous_object_keys_for_band(self, band: str, time: datetime, retries: int = 3) -> list[str]:
        if retries < 0:
            return []
        object_keys = list(self._get_all_object_keys_for_band_in_aws_directory(band, time))
        best_object_start_time = None
        for object_key in object_keys:
            object_start_time = self._get_scan_start_time_from_object_key(object_key)
            if object_start_time > time:
                continue
            elif best_object_start_time is None or object_start_time > best_object_start_time:
                best_object_start_time = object_start_time
        if best_object_start_time is None or best_object_start_time > time:
            return self._get_previous_object_keys_for_band(band, time - self.update_frequency, retries - 1)
        return list(filter(lambda key: self._get_scan_start_time_from_object_key(key) == best_object_start_time, object_keys))

    def _get_previous_scan_start_time_for_band(self, band: str, time: datetime) -> datetime:
        object_key = self._get_previous_object_keys_for_band(band, time)[0]
        return self._get_scan_start_time_from_object_key(object_key)

    def _get_previous_keys_for_bands(self, bands: [str], time: datetime) -> [[str]]:
        return [self._get_previous_object_keys_for_band(band, time) for band in bands]

    def _download(self, bands: [str], time: datetime) -> [str]:
        keys_list = self._get_previous_keys_for_bands(bands, time)
        file_paths = []
        s3t = s3transfer.create_transfer_manager(self.s3_client, self.transfer_config)
        for keys in keys_list:
            for key in keys:
                file_path = self.get_local_file_path(key)
                if not self.file_is_downloaded(key):
                    s3t.download(self.bucket, key, file_path)
                file_paths.append(file_path)
        s3t.shutdown()
        return list(map(self._file_posthandler, file_paths))
