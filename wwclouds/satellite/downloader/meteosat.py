import os.path
from datetime import datetime, timedelta
import requests
from urllib.parse import quote_plus
from enum import Enum, auto
from typing import Union, List

from wwclouds.satellite.downloader.downloader import Downloader
from wwclouds.satellite.downloader.file_reader import FileReader
from wwclouds import config
from wwclouds.satellite.satellite_enum import SatelliteEnum


class MeteosatType(Enum):
    METEOSAT8 = auto()
    METEOSAT11 = auto()

    @property
    def collection_id(self):
        if self == MeteosatType.METEOSAT8:
            return "EO:EUM:DAT:MSG:HRSEVIRI-IODC"
        elif self == MeteosatType.METEOSAT11:
            return "EO:EUM:DAT:MSG:HRSEVIRI"

    @staticmethod
    def from_str(string: str) -> "MeteosatType":
        return getattr(MeteosatType, string)

    @staticmethod
    def from_satellite_flag(satellite_enum: SatelliteEnum) -> "MeteosatType":
        return MeteosatType.from_str(satellite_enum.name)


class Meteosat(Downloader):
    def __init__(self, satellite_enum: SatelliteEnum):
        meteosat_type = MeteosatType.from_satellite_flag(satellite_enum)
        super().__init__(
            subdir=f"{meteosat_type.name.lower()}/{meteosat_type.collection_id}",
            reader="seviri_l1b_native",
            update_frequency=timedelta(minutes=15)
        )
        self.collection_id = meteosat_type.collection_id

    @property
    def url_friendly_collection_id(self) -> str:
        return quote_plus(self.collection_id)

    def __get_access_key(self) -> str:
        token_url = config.METEOSAT_API_ENDPOINT + "/token"
        response = requests.post(
            token_url,
            auth=requests.auth.HTTPBasicAuth(config.METEOSAT_CONSUMER_KEY, config.METEOSAT_CONSUMER_SECRET),
            data={'grant_type': 'client_credentials'},
            headers={"Content-Type": "application/x-www-form-urlencoded"}
        )
        return response.json()['access_token']

    def __get_product_url_for_time(self, time: datetime) -> str:
        url_ending = f"collections/{self.url_friendly_collection_id}/dates/{time.year}/{time.month:02.0f}" \
                     f"/{time.day:02.0f}/times/{time.hour:02.0f}/{time.minute:02.0f}/products"
        return f"{config.METEOSAT_BROWSE_ENDPOINT}/{url_ending}"

    def __get_product_info_for_time(self, time: datetime) -> dict:
        url = self.__get_product_url_for_time(time)
        response = requests.get(url, params={"format": "json"})
        return response.json()

    def __get_product_id_for_time(self, time: datetime, retries: int = 3) -> str:
        info = self.__get_product_info_for_time(time)
        products = info.get("products")
        if not products and retries >= 1:
            return self.__get_product_id_for_time(time - self.update_frequency, retries - 1)
        return products[0]["id"]

    def __get_download_url_for_product(self, product_id: str) -> str:
        url_ending = f"collections/{self.url_friendly_collection_id}/products/{product_id}/entry?name={product_id}.nat"
        return f"{config.METEOSAT_DOWNLOAD_ENDPOINT}/{url_ending}"

    def __get_download_url_for_time(self, time: datetime) -> str:
        product_id = self.__get_product_id_for_time(time)
        return self.__get_download_url_for_product(product_id)

    def _download(self, bands: Union[List[int], None], time: datetime) -> [str]:
        download_url = self.__get_download_url_for_time(time)
        filepath = self.get_local_file_path(download_url)
        if not self.file_is_downloaded(download_url):
            access_token = self.__get_access_key()
            stream_response = requests.get(
                download_url,
                params={"format": "json"},
                stream=True,
                headers={"Authorization": f"Bearer {access_token}"})
            with open(filepath, "wb") as f:
                for chunk in stream_response.iter_content(chunk_size=1024):
                    if chunk:
                        f.write(chunk)
                        f.flush()
        return [filepath]


if __name__ == '__main__':
    meteosat = Meteosat(SatelliteEnum.METEOSAT8)
    file_reader = meteosat.download(time=datetime(2022, 1, 12, 15, 29))
    scn = file_reader.read_to_scene()
    print(scn.available_dataset_ids())
    my_scene = 1.5
    scn.load([my_scene])

    import matplotlib.pyplot as plt
    plt.figure()
    plt.imshow(scn[my_scene])
    plt.show()
