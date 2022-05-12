from enum import Enum


class SatelliteEnum(Enum):
    METEOSAT8 = 1 << 0
    METEOSAT11 = 1 << 1
    GOES16 = 1 << 2
    GOES17 = 1 << 3
    HIMAWARI8 = 1 << 4

    @classmethod
    def all(cls) -> ["SatelliteEnum"]:
        return [satellite_enum for satellite_enum in cls]
