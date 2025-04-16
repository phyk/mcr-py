import os
import pickle
from typing import List, Tuple

import folium
import polars as pl
import polars_st as st
import pyproj
import shapely.geometry.base
import shapely.ops
from shapely.geometry import MultiPolygon, Polygon

from mcr_py.package.utils import cache


def convert_to_crs(
    geometry: shapely.geometry.base.BaseGeometry, crs: str, crs_target: str
):
    """
    Convert the geometry from one coordinate reference system (CRS) to another.

    geometry: The geometry to be transformed.
    crs: The source CRS as a string (e.g., "EPSG:4326").
    crs_target: The target CRS as a string (e.g., "EPSG:3857").

    :returns: The transformed geometry in the target CRS.
    """
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
        """
        Initialize the GeoMeta object with a boundary, source CRS, and target CRS.

        boundary: The polygon representing the boundary.
        crs: The source CRS as a string.
        crs_target: The target CRS as a string.
        """
        self.crs = crs
        self.crs_target = crs_target
        self.unbuffered_boundary = boundary
        buffered_boundary = convert_to_crs(self.unbuffered_boundary, crs, crs_target)
        buffered_boundary = buffered_boundary.buffer(self.BUFFER)
        self.boundary = convert_to_crs(buffered_boundary, crs_target, crs)
        self.residential_area = None

    def hash_boundary(self):
        """
        Generate a hash string for the boundary geometry.

        :returns: A hash string representing the boundary.
        """
        return cache.hash_str(self.boundary.wkt)

    def get_bounding_box(self, use_buffer: bool = True):
        """
        Get the bounding box of the boundary.

        use_buffer: Whether to use the buffered boundary or the unbuffered boundary.

        :returns: A tuple representing the bounding box (minx, miny, maxx, maxy).
        """
        if use_buffer:
            return self.boundary.bounds
        else:
            return self.unbuffered_boundary.bounds

    def get_bounding_box_as_coord_list(
        self, use_buffer: bool = True
    ) -> List[Tuple[float, float]]:
        """
        Get the bounding box as a list of coordinates.

        use_buffer: Whether to use the buffered boundary or the unbuffered boundary.

        :returns: A list of tuples representing the corners of the bounding box.
        """
        bounding_box = self.get_bounding_box(use_buffer=use_buffer)
        return [
            (bounding_box[0], bounding_box[1]),
            (bounding_box[0], bounding_box[3]),
            (bounding_box[2], bounding_box[3]),
            (bounding_box[2], bounding_box[1]),
        ]

    @staticmethod
    def load(path: str):
        """
        Load a GeoMeta object from a pickle file.

        path: The path to the pickle file containing the GeoMeta object.

        :returns: The loaded GeoMeta object.
        :raises ValueError: If the loaded object is not a GeoMeta.
        """
        with open(path, "rb") as f:
            loaded = pickle.load(f)
            if not isinstance(loaded, GeoMeta):
                raise ValueError(f"File at {path} does not contain a GeoMeta object.")
            return loaded

    def set_residential_area(self, residential_area: MultiPolygon):
        """
        Set the residential area for the GeoMeta object.

        residential_area: A MultiPolygon representing the residential area.
        """
        self.residential_area = residential_area

    def save(self, path: str):
        """
        Save the GeoMeta object to a pickle file.

        path: The path where the GeoMeta object will be saved.
        """
        os.makedirs(os.path.dirname(path), exist_ok=True)

        with open(path, "wb") as f:
            pickle.dump(self, f)

    def crop_gdf(
        self, locations: pl.DataFrame, use_buffer: bool = True
    ) -> pl.DataFrame:
        """
        Crop a GeoDataFrame to the boundaries defined in the GeoMeta object.

        locations: The input GeoDataFrame to be cropped.
        use_buffer: Whether to use the buffered boundary or the unbuffered boundary.

        :returns: A cropped GeoDataFrame containing only the locations within the boundary.
        """
        boundary = self.unbuffered_boundary
        if use_buffer:
            boundary = self.boundary
        locations = locations.filter(
            st.geom("geometry").st.within(st.from_wkt(pl.lit(boundary.wkt)))
        )

        return locations

    def crop_df(
        self,
        locations: pl.DataFrame,
        lat_col: str,
        lon_col: str,
        use_buffer: bool = True,
    ) -> pl.DataFrame:
        """
        Crop a DataFrame of locations to the boundaries defined in the GeoMeta object.

        locations: The input DataFrame to be cropped.
        lat_col: The name of the latitude column.
        lon_col: The name of the longitude column.
        use_buffer: Whether to use the buffered boundary or the unbuffered boundary.

        :returns: A cropped DataFrame containing only the locations within the boundary.
        """
        boundary = self.unbuffered_boundary
        if use_buffer:
            boundary = self.boundary

        locations = locations.filter(
            st.from_coords(pl.concat_arr(lon_col, lat_col)).st.within(
                st.from_wkt(pl.lit(boundary.wkt))
            )
        )

        return locations

    def get_center_lat_lon(self) -> tuple[float, float]:
        """
        Get the latitude and longitude of the centroid of the boundary.

        :returns: A tuple containing the latitude and longitude of the centroid.
        """
        lon, lat = self.boundary.centroid.coords[0]
        return lat, lon

    def add_to_folium_map(self, m: folium.Map) -> folium.Map:
        """
        Add the boundary and residential area to a Folium map.

        m: The Folium map to which the geometries will be added.

        :returns: The updated Folium map with added geometries.
        """
        folium.GeoJson(self.boundary).add_to(m)
        folium.GeoJson(self.unbuffered_boundary).add_to(m)

        if self.residential_area is not None:
            folium.GeoJson(self.residential_area).add_to(m)
        return m
