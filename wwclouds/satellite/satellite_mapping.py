from typing import Type
from wwclouds.satellite import satellite_type
from wwclouds.satellite.satellite_enum import SatelliteEnum


class _SatelliteMappingEntry:
    def __init__(self, satellite_type_class: Type[satellite_type.SatelliteType]):
        self.satellite_type_class = satellite_type_class


class SatelliteMapping:
    _MAPPING = {
        SatelliteEnum.METEOSAT8: _SatelliteMappingEntry(satellite_type.Meteosat),
        SatelliteEnum.METEOSAT11: _SatelliteMappingEntry(satellite_type.Meteosat),
        SatelliteEnum.GOES16: _SatelliteMappingEntry(satellite_type.NoaaGoes),
        SatelliteEnum.GOES17: _SatelliteMappingEntry(satellite_type.NoaaGoes),
        SatelliteEnum.HIMAWARI8: _SatelliteMappingEntry(satellite_type.Himawari)
    }

    @staticmethod
    def __get_entry(satellite_enum: SatelliteEnum) -> _SatelliteMappingEntry:
        satellite_enum_copy = SatelliteEnum(satellite_enum.value)
        mapping_entry = SatelliteMapping._MAPPING.get(satellite_enum_copy)
        if mapping_entry is None:
            raise NotImplementedError("satellite_enum mapping is not implemented for the given satellite")
        return mapping_entry

    @staticmethod
    def get_satellite_type(satellite_enum: SatelliteEnum) -> satellite_type.SatelliteType:
        mapping_entry = SatelliteMapping.__get_entry(satellite_enum)
        return mapping_entry.satellite_type_class(satellite_enum=satellite_enum)
