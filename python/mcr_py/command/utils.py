import pathlib

from mcr_py.mcr.data import NetworkType, OSMData, RedownloadMode
from mcr_py.utils.geometa import GeoMeta


def load_auxiliary_classes(
    geo_meta_path: pathlib.Path, city_id: str, osm_path: pathlib.Path, cache_path: pathlib.Path
) -> tuple[GeoMeta, OSMData]:
    geo_meta = GeoMeta.load(geo_meta_path)
    geo_data = OSMData(
        geo_meta,
        city_id,
        cache_path=cache_path,
        osm_path=osm_path,
        redownload=RedownloadMode.REUSE,
        additional_network_types=[NetworkType.CYCLING, NetworkType.DRIVING],
    )
    return geo_meta, geo_data
