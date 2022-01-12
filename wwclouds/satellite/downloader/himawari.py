import os.path
from datetime import datetime, timedelta
import bz2

from wwclouds.satellite.downloader.aws import Aws


class Himawari(Aws):
    def __init__(self):
        super().__init__(
            bucket="noaa-himawari8",
            product="AHI-L1b-FLDK",
            reader="ahi_hsd",
            update_frequency=timedelta(minutes=10),
            all_bands=list(range(1, 17)))
        self.image_part_count = 10

    def __get_aws_directory(self, update_time: datetime) -> str:
        return f"{self.product}/{update_time.year}/{update_time.month:02.0f}/{update_time.day:02.0f}" \
               f"/{update_time.hour:02.0f}{update_time.minute:02.0f}"

    def _get_aws_prefix_for_band(self, band: int, update_time: datetime) -> str:
        return f"{self.__get_aws_directory(update_time)}/HS_H08_{update_time.year}{update_time.month:02.0f}" \
               f"{update_time.day:02.0f}_{update_time.hour:02.0f}{update_time.minute:02.0f}_B{band:02.0f}_FLDK"

    def _get_latest_keys_for_band(self, band: int, time: datetime, retries: int = 3) -> [str]:
        prev_updated_time = self.get_previous_update_time(time)
        file_entries = list(self._get_all_file_entries_for_band_in_aws_directory(band, prev_updated_time))
        if len(file_entries) != self.image_part_count and retries >= 1:
            return self._get_latest_keys_for_band(band, time - self.update_frequency, retries - 1)
        return list(map(lambda file_entry: file_entry.key, file_entries))

    def _file_posthandler(self, filepath: str) -> str:
        new_filepath = filepath.rstrip(".bz2")
        if not os.path.exists(new_filepath):
            with open(new_filepath, "wb") as new_file, bz2.BZ2File(filepath, "rb") as file:
                for data in iter(lambda: file.read(100 * 1024), b""):
                    new_file.write(data)
        return new_filepath


if __name__ == "__main__":
    himawari = Himawari()
    file_reader = himawari.download([6, 7, 8], datetime.utcnow())
    print(file_reader.filepaths)
    scn = file_reader.read_to_scene()
    print(scn.available_dataset_names())
    my_scene = "B07"
    scn.load([my_scene])

    import matplotlib.pyplot as plt
    plt.figure()
    plt.imshow(scn[my_scene])
    plt.show()
