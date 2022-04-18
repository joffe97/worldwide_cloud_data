from wwclouds.helpers.axis_helper import AxisHelper


class LatitudeHelper(AxisHelper):
    @staticmethod
    def is_between(lat: float, lat_a: float, lat_b: float) -> bool:
        lat_min, lat_max = sorted((lat_a, lat_b))
        return lat_min <= lat <= lat_max

    @staticmethod
    def get_diff(a: float, b: float) -> float:
        min_val, max_val = sorted((a, b))
        return max_val - min_val

    @staticmethod
    def get_middle(a: float, b: float) -> float:
        return (a + b) / 2
