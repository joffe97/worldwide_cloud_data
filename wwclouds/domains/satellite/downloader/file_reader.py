import satpy


class FileReader:
    def __init__(self, filepaths: [str], reader: str):
        self.filepaths = filepaths
        self.reader = reader

    def read_to_scene(self) -> satpy.Scene:
        return satpy.Scene(filenames=self.filepaths, reader=self.reader)
