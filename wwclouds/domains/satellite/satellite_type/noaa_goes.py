from wwclouds.domains.satellite.satellite_type import SatelliteType
from wwclouds.domains.satellite import downloader
from wwclouds.domains.satellite.satellite_enum import SatelliteEnum


class NoaaGoes(SatelliteType):
    def __init__(self, satellite_enum: SatelliteEnum):
        frequency_band_mapping = {
            "01": (0.45, 0.49),
            "02": (0.59, 0.69),
            "03": (0.846, 0.885),
            "04": (1.371, 1.386),
            "05": (1.58, 1.64),
            "06": (2.225, 2.275),
            "07": (3.80, 4.00),
            "08": (5.77, 6.6),
            "09": (6.75, 7.15),
            "10": (7.24, 7.44),
            "11": (8.3, 8.7),
            "12": (9.42, 9.8),
            "13": (10.1, 10.6),
            "14": (10.8, 11.6),
            "15": (11.8, 12.8),
            "16": (13.0, 13.6),
        }
        super().__init__(downloader.NoaaGoes(satellite_enum), frequency_band_mapping)
