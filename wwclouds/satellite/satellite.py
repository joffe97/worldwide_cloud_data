from satpy import Scene

from .downloader import file_reader


class Satellite:
    def __init__(self):
        self.file_reader: sat_file_reader.FileReader | None = None

    def download_data(self):
        raise NotImplementedError("Download method is not implemented for this satellite")

    def get_scene(self) -> Scene:
        self.download_data()
        return self.file_reader.read_to_scene()
