from enum import Enum, auto


class Axis(Enum):
    LON = auto()
    LAT = auto()

    @property
    def length_in_metres(self) -> float:
        if self == Axis.LON:
            return 40_075_017.0
        elif self == Axis.LAT:
            return 20_003_931.5
        else:
            raise ValueError("earth_radius_in_metres is not implemented for the given axis")

    @property
    def degree_count(self):
        if self == Axis.LON:
            return 360
        elif self == Axis.LAT:
            return 180
        else:
            raise ValueError("degree_count is not implemented for the given axis")
