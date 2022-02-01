from enum import Enum, auto
from datetime import datetime, timedelta

from wwclouds.satellite.downloader.aws import Aws
from wwclouds.satellite.satellite_enum import SatelliteEnum


class NoaaGoesType(Enum):
    GOES16 = auto()
    GOES17 = auto()

    @property
    def bucket_name(self):
        return f"noaa-{self.name.lower()}"

    @staticmethod
    def from_str(string: str) -> "NoaaGoesType":
        return eval(f"NoaaGoesType.{string}")

    @staticmethod
    def from_satellite_flag(satellite_enum: SatelliteEnum) -> "NoaaGoesType":
        return NoaaGoesType.from_str(satellite_enum.name)


class NoaaGoes(Aws):
    def __init__(self, satellite_enum: SatelliteEnum):
        noaa_goes_type = NoaaGoesType.from_satellite_flag(satellite_enum)
        super().__init__(
            bucket=noaa_goes_type.bucket_name,
            product="ABI-L1b-RadF",
            reader="abi_l1b",
            update_frequency=timedelta(minutes=10),
            all_bands=list(range(1, 17)))

    def __get_aws_directory(self, time: datetime) -> str:
        day_of_year = time.timetuple().tm_yday
        return f"{self.product}/{time.year}/{day_of_year:03.0f}/{time.hour:02.0f}"

    def _get_aws_prefix_for_band(self, band: int, time: datetime) -> str:
        return f"{self.__get_aws_directory(time)}/OR_{self.product}-M6C{band:02.0f}"

    def _get_previous_keys_for_band(self, band: int, time: datetime, retries: int = 3) -> [str]:
        file_entries = list(self._get_all_file_entries_for_band_in_aws_directory(band, time))
        if not file_entries and retries >= 1:
            return self._get_previous_keys_for_band(band, time - self.update_frequency, retries - 1)
        return [sorted(file_entries, key=lambda file_entry: file_entry.time)[-1].key]


if __name__ == "__main__":
    satellite_enum = SatelliteEnum.GOES16
    noaa_goes = NoaaGoes(satellite_enum)
    file_reader = noaa_goes.download(time=datetime(2022, 1, 12, 15, 29))
    print(file_reader.filepaths)
    scn = file_reader.read_to_scene()
    print(scn.available_dataset_names())
    my_scene = "C06"
    scn.load([my_scene])

    import matplotlib.pyplot as plt
    plt.figure()
    plt.imshow(scn[my_scene])
    plt.show()
