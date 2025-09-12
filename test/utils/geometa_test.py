import pathlib

import polars as pl
import polars_st as st
import shapely.geometry
from mcr_py.utils import cache
from mcr_py.utils.geometa import GeoMeta


def test_initialization(geo_meta: GeoMeta) -> None:
    assert geo_meta.crs == "EPSG:4326"
    assert geo_meta.crs_target == "EPSG:3857"
    assert geo_meta.boundary_wkt is not None


def test_hash_boundary(geo_meta: GeoMeta) -> None:
    boundary_hash = geo_meta.hash_boundary()
    assert isinstance(boundary_hash, int)
    assert boundary_hash == cache.hash_str(geo_meta.boundary_wkt)


def test_get_bounding_box(geo_meta: GeoMeta) -> None:
    bbox = geo_meta.get_bounding_box()
    assert len(bbox) == 4  # (minx, miny, maxx, maxy)


def test_get_bounding_box_as_coord_list(geo_meta: GeoMeta) -> None:
    coord_list = geo_meta.get_bounding_box_as_coord_list()
    assert len(coord_list) == 4  # Should return 4 coordinates


def test_load_and_save(geo_meta: GeoMeta, tmp_path: pathlib.Path) -> None:
    # Test saving
    save_path = tmp_path / "geometa_data.json"
    geo_meta.save(save_path)

    # Test loading
    loaded_geo_meta = GeoMeta.load(save_path)
    assert loaded_geo_meta.hash_boundary() == geo_meta.hash_boundary()


def test_crop_gdf(geo_meta: GeoMeta) -> None:
    # Create a sample DataFrame
    data = {"geometry": [shapely.geometry.Point(0.5, 0.5), shapely.geometry.Point(2, 2)]}
    locations = pl.DataFrame(data)
    locations = locations.select(st.from_shapely("geometry"))
    cropped_locations = geo_meta.crop_gdf(locations)

    assert len(cropped_locations) == 1  # Only one point should be inside the boundary


def test_crop_df(geo_meta: GeoMeta) -> None:
    # Create a sample DataFrame
    data = {"lat": [0.5, 2], "lon": [0.5, 2]}
    locations = pl.DataFrame(data)
    cropped_locations = geo_meta.crop_df(locations, lat_col="lat", lon_col="lon")

    assert len(cropped_locations) == 1  # Only one point should be inside the boundary


def test_get_center_lat_lon(geo_meta: GeoMeta) -> None:
    center = geo_meta.get_center_lat_lon()
    assert len(center) == 2  # Should return (lat, lon)
    assert isinstance(center[0], float)  # Latitude
    assert isinstance(center[1], float)  # Longitude


def test_add_to_folium_map(geo_meta: GeoMeta) -> None:
    import folium

    m = folium.Map(location=[0, 0], zoom_start=2)
    updated_map = geo_meta.add_to_folium_map(m)

    assert isinstance(updated_map, folium.Map)
    assert len(updated_map._children) > 0  # Should have added GeoJson layers
