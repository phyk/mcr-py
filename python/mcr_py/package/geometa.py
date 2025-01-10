import os
import pickle

import folium
import geopandas as gpd
import pandas as pd
import pyproj
import shapely.geometry.base
import shapely.ops
from shapely.geometry import MultiPolygon, Point, Polygon

from mcr_py.package import cache


def convert_to_crs(
    geometry: shapely.geometry.base.BaseGeometry, crs: str, crs_target: str
):
    crs_source = pyproj.CRS(crs)
    crs_sink = pyproj.CRS(crs_target)
    transform_source_sink = pyproj.Transformer.from_crs(
        crs_source, crs_sink, always_xy=True
    ).transform
    return shapely.ops.transform(transform_source_sink, geometry)


class GeoMeta:
    """
    GeoMeta incorporates general geospatial information, including the boundary of the area of consideration.
    """

    BUFFER = 10000  # roughly 5km

    def __init__(self, boundary: Polygon, crs: str, crs_target: str):
        self.crs = crs
        self.crs_target = crs_target
        self.unbuffered_boundary = boundary
        buffered_boundary = convert_to_crs(self.unbuffered_boundary, crs, crs_target)
        buffered_boundary = buffered_boundary.buffer(self.BUFFER)
        self.boundary = convert_to_crs(buffered_boundary, crs_target, crs)
        self.residential_area = None

    def hash_boundary(self):
        return cache.hash_str(self.boundary.wkt)

    def get_bounding_box(self, use_buffer: bool = True):
        if use_buffer:
            return self.boundary.bounds
        else:
            return self.unbuffered_boundary.bounds

    @staticmethod
    def load(path: str):
        with open(path, "rb") as f:
            loaded = pickle.load(f)
            if not isinstance(loaded, GeoMeta):
                raise ValueError(f"File at {path} does not contain a GeoMeta object.")
            return loaded

    def set_residential_area(self, residential_area: MultiPolygon):
        self.residential_area = residential_area

    def save(self, path: str):
        os.makedirs(os.path.dirname(path), exist_ok=True)

        with open(path, "wb") as f:
            pickle.dump(self, f)

    def crop_gdf(
        self, locations: gpd.GeoDataFrame, use_buffer: bool = True
    ) -> gpd.GeoDataFrame:
        boundary = self.unbuffered_boundary
        if use_buffer:
            boundary = self.boundary

        locations = locations.loc[locations.geometry.within(boundary), :]

        return locations

    def crop_df(
        self,
        locations: pd.DataFrame,
        lat_col: str,
        lon_col: str,
        use_buffer: bool = True,
    ) -> pd.DataFrame:
        boundary = self.unbuffered_boundary
        if use_buffer:
            boundary = self.boundary

        locations = locations.loc[
            locations.apply(
                lambda x: boundary.contains(Point(x[lon_col], x[lat_col])),
                axis=1,
            ),
            :,
        ]

        return locations

    def get_center_lat_lon(self) -> tuple[float, float]:
        lon, lat = self.boundary.centroid.coords[0]
        return lat, lon

    def add_to_folium_map(self, m: folium.Map) -> folium.Map:
        folium.GeoJson(self.boundary).add_to(m)
        folium.GeoJson(self.unbuffered_boundary).add_to(m)

        if self.residential_area is not None:
            folium.GeoJson(self.residential_area).add_to(m)
        return m
