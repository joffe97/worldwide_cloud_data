from typing import Union
import cv2
import numpy as np
import pathlib


class ImageVisual:
    def __init__(self, resolution: tuple[int, int], *, load: bool = False):
        if not isinstance(resolution, tuple) or len(resolution) != 2 or not all(isinstance(dim, int) for dim in resolution):
            raise ValueError("resolution is invalid")

        self.resolution = resolution
        self.__image: Union[..., None] = None

        if load:
            self.load()

    @staticmethod
    def from_image(image, **kwargs) -> "ImageVisual":
        resolution = tuple(int(image.shape[i]) for i in [1, 0])
        return ImageVisual(resolution, **kwargs)

    @staticmethod
    def from_image_path(image_path: str, **kwargs) -> ("ImageVisual", np.ndarray):
        image = cv2.imread(image_path, cv2.IMREAD_UNCHANGED)
        world_map = ImageVisual.from_image(image, **kwargs)
        return world_map, image

    @property
    def directory(self) -> str:
        return str(pathlib.Path(__file__).parent.resolve())

    @property
    def filename(self) -> str:
        return "world_map.png"

    @property
    def filepath(self) -> str:
        return f"{self.directory}/{self.filename}"

    @property
    def shapefile_path(self) -> str:
        return f"{self.directory}/world_borders.shp"

    @property
    def image(self):
        if self.__image is None:
            raise ValueError("image cannot be used before it's loaded")
        return self.__image

    def load(self):
        image = cv2.imread(self.filepath, cv2.IMREAD_UNCHANGED)
        image_resized = cv2.resize(image, self.resolution)
        self.__image = image_resized

    def add_4dim_image(self, image) -> None:
        image_resolution = (image.shape[1], image.shape[0])
        x_ratio = self.resolution[0] / image_resolution[0]
        resolution_resized = tuple(int(dim * x_ratio) for dim in image_resolution)
        image_resized = cv2.resize(image, resolution_resized)

        y_offset = (self.resolution[1] - image_resized.shape[0]) / 2
        s_img = image_resized
        l_img = self.image

        if y_offset < 0:
            new_image = np.empty(s_img.shape, dtype=np.uint8)
            new_image[:, :, :] = 0.0
            new_y1, new_y2 = int(-y_offset), int(y_offset + s_img.shape[0])
            for c in range(4):
                new_image[new_y1:new_y2, :, c] = l_img[:, :, c]
            l_img = new_image
            y_offset = 0

        y1, y2 = int(y_offset), int(y_offset + s_img.shape[0])
        alpha_s = s_img[:, :, 3] / 255.0
        alpha_l = 1.0 - alpha_s

        for c in range(0, 3):
            self.__image = l_img
            self.__image[y1:y2, :, c] = (alpha_s * s_img[:, :, c] + alpha_l * l_img[y1:y2, :, c])

    def add_4dim_image_from_png(self, filepath: str):
        image = cv2.imread(filepath, cv2.IMREAD_UNCHANGED)
        self.add_4dim_image(image)

    def save_as_png(self, filepath):
        cv2.imwrite(filepath, self.image)


if __name__ == '__main__':
    wwmap = ImageVisual((10000, 5000), load=True)
    wwmap.create_image()
