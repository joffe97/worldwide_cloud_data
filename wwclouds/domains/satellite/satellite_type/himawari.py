from wwclouds.domains.satellite.satellite_type import SatelliteType
from wwclouds.domains.satellite import downloader


class Himawari(SatelliteType):
    def __init__(self, **kwargs):
        band_frequency_mapping = {
            "01": (0.43, 0.48),
            "02": (0.50, 0.52),
            "03": (0.63, 0.66),
            "04": (0.85, 0.87),
            "05": (1.60, 1.62),
            "06": (2.25, 2.27),
            "07": (3.74, 3.96),
            "08": (6.06, 6.43),
            "09": (6.89, 7.01),
            "10": (7.26, 7.43),
            "11": (8.44, 8.76),
            "12": (9.54, 9.72),
            "13": (10.3, 10.6),
            "14": (11.1, 11.3),
            "15": (12.2, 12.5),
            "16": (13.2, 13.4)
        }
        super().__init__(downloader.Himawari(), band_frequency_mapping)
