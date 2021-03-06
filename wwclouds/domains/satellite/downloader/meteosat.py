import functools
from datetime import datetime, timedelta
import requests
from urllib.parse import quote_plus
from enum import Enum, auto
from typing import List, Optional

from wwclouds.domains.satellite.downloader.downloader import Downloader
from wwclouds import config
from wwclouds.domains.satellite.satellite_enum import SatelliteEnum


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
    def __url_friendly_collection_id(self) -> str:
        return quote_plus(self.collection_id)

    def _get_previous_scan_start_time_for_band(self, band: str, time: datetime):
        return self._get_previous_update_time(time)

    def __get_access_token(self) -> str:
        response = requests.post(
            url=config.METEOSAT_TOKEN_ENDPOINT,
            auth=requests.auth.HTTPBasicAuth(config.METEOSAT_CONSUMER_KEY, config.METEOSAT_CONSUMER_SECRET),
            data={'grant_type': 'client_credentials'},
            headers={"Content-Type": "application/x-www-form-urlencoded"}
        )
        response_json = response.json()
        access_token = response_json.get('access_token')
        if not access_token:
            recived_errors = []
            for key in "error", "error_description":
                if key in response_json:
                    recived_errors.append(response_json[key])
            recived_error_msg = ": ".join(recived_errors)
            raise PermissionError(f"access token not recived. {recived_error_msg}")
        return access_token

    def __get_product_url_for_time(self, time: datetime) -> str:
        url_ending = f"collections/{self.__url_friendly_collection_id}/dates/{time.year}/{time.month:02.0f}" \
                     f"/{time.day:02.0f}/times/{time.hour:02.0f}/{time.minute:02.0f}/products"
        return f"{config.METEOSAT_BROWSE_ENDPOINT}/{url_ending}"

    def __get_product_info_for_time(self, time: datetime) -> dict:
        url = self.__get_product_url_for_time(time)
        response = requests.get(url, params={"format": "json"})
        return response.json()

    def __get_product_id_for_time(self, time: datetime, retries: int = 3) -> Optional[str]:
        if retries <= 0:
            return None
        info = self.__get_product_info_for_time(time)
        products = info.get("products")
        if not products:
            return self.__get_product_id_for_time(time - self.update_frequency, retries - 1)
        product = products[0]
        return product["id"]

    def __get_download_url_for_product(self, product_id: str) -> str:
        url_ending = f"collections/{self.__url_friendly_collection_id}/products/{product_id}/entry?name={product_id}.nat"
        return f"{config.METEOSAT_DOWNLOAD_ENDPOINT}/{url_ending}"

    @functools.lru_cache(32)
    def __get_download_url_for_time(self, time: datetime) -> str:
        product_id = self.__get_product_id_for_time(time)
        return self.__get_download_url_for_product(product_id)

    def _download(self, bands: Optional[List[str]], time: datetime) -> [str]:
        previous_updated_time = self._get_previous_update_time(time)
        download_url = self.__get_download_url_for_time(previous_updated_time)
        filepath = self._get_local_file_path(download_url)
        if not self._file_is_downloaded(download_url):
            access_token = self.__get_access_token()
            stream_response = requests.get(
                url=download_url,
                params={"format": "json"},
                stream=True,
                headers={"Authorization": f"Bearer {access_token}"})
            with open(filepath, "wb") as f:
                for chunk in stream_response.iter_content(chunk_size=1024):
                    if chunk:
                        f.write(chunk)
                        f.flush()
        return [filepath]
