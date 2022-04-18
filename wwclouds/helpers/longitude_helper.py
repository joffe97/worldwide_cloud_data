import functools
from wwclouds.data_types.axis import Axis
from wwclouds.helpers.axis_helper import AxisHelper


class LongitudeHelper(AxisHelper):
    @staticmethod
    def add(lon: float, num: float) -> float:
        fixed_lon = lon + Axis.LON.degree_count / 2
        fixed_result = (fixed_lon + num) % Axis.LON.degree_count
        return fixed_result - (Axis.LON.degree_count / 2)

    @staticmethod
    def get_diff(lon_a: float, lon_b: float) -> float:
        lon_low, lon_high = sorted((lon_a, lon_b))
        lon_diff = lon_high - lon_low
        return min(lon_diff, Axis.LON.degree_count - lon_diff)

    @staticmethod
    @functools.lru_cache(64)
    def get_middle(lon_a: float, lon_b: float) -> float:
        lon_diff = LongitudeHelper.get_diff(lon_a, lon_b)
        lon_middles = []
        for lon in lon_a, lon_b:
            cur_lon_middle = lon + (lon_diff / 2)
            if cur_lon_middle > Axis.LON.degree_count / 2:
                cur_lon_middle -= Axis.LON.degree_count
            lon_middles.append(cur_lon_middle)
        lon_middle = min(
            lon_middles,
            key=lambda lon: LongitudeHelper.get_diff(lon_a, lon) + LongitudeHelper.get_diff(lon_b, lon)
        )
        return lon_middle

    @staticmethod
    def is_between(lon: float, lon_a: float, lon_b: float) -> bool:
        range_width = LongitudeHelper.get_diff(lon_a, lon_b)
        lon_middle = LongitudeHelper.get_middle(lon_a, lon_b)
        return LongitudeHelper.get_diff(lon_middle, lon) <= range_width / 2
