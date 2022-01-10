import satpy
import boto3
from botocore import UNSIGNED
from botocore.config import Config
from datetime import datetime, timedelta
from typing import Iterator

from wwclouds.satellite.downloader.downloader import Downloader
from wwclouds.satellite.downloader.file_reader import FileReader


class FileEntry:
    def __init__(self, key: str, time: datetime):
        self.key = key
        self.time = time


class Aws(Downloader):
    s3_client = boto3.client("s3", config=Config(signature_version=UNSIGNED))

    def __init__(self, bucket: str, bands: [int], time: datetime = datetime.utcnow()):
        self.bucket = bucket
        self.bands = bands
        self.time = datetime(time.year, time.month, time.day, time.hour)
        self.product = "ABI-L1b-RadF"
        super().__init__(subdir=f"{self.bucket}")

    def __get_aws_directory(self, time: datetime) -> str:
        day_of_year = time.timetuple().tm_yday
        return f"{self.product}/{time.year}/{day_of_year:03.0f}/{time.hour:02.0f}"

    def __get_aws_location_for_band(self, band: int, time: datetime) -> str:
        return f"{self.__get_aws_directory(time)}/OR_{self.product}-M6C{band:02.0f}"

    def __get_all_file_entries_for_band_in_hour(self, band: int, time: datetime) -> Iterator[FileEntry]:
        prefix = self.__get_aws_location_for_band(band, time)
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

    def __get_latest_key_for_band(self, band: int) -> str:
        time = self.time
        keys = list(self.__get_all_file_entries_for_band_in_hour(band, time))
        if not keys:
            time = time - timedelta(hours=1)
            keys = list(self.__get_all_file_entries_for_band_in_hour(band, time))
        return sorted(keys, key=lambda key: key.time)[-1].key

    def __get_latest_keys(self) -> [str]:
        return [self.__get_latest_key_for_band(band) for band in self.bands]

    def __get_local_file_path(self, key: str) -> str:
        file_name = key.split("/")[-1]
        return f"{self.path}/{file_name}"

    def download(self) -> [str]:
        keys = self.__get_latest_keys()
        for key in keys:
            file_path = self.__get_local_file_path(key)
            with open(file_path, "wb") as f:
                self.s3_client.download_fileobj(self.bucket, key, f)
            yield file_path


if __name__ == "__main__":
    aws = Aws("noaa-goes16", [6], datetime.utcnow())
    paths = list(aws.download())
    print(paths)
    scn = satpy.Scene(filenames=paths, reader="abi_l1b")
    my_scene = "C06"
    scn.load([my_scene])
    import matplotlib.pyplot as plt
    plt.figure()
    plt.imshow(scn[my_scene])
    plt.show()
