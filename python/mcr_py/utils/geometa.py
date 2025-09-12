import pathlib
from dataclasses import dataclass
from enum import Enum

import folium
import polars as pl
import polars_st as st
import pyproj
import shapely.geometry.base
import shapely.ops
from beartype.typing import List, Tuple
from serde import serde
from serde.json import from_json, to_json
from shapely.geometry import MultiPolygon, Polygon

from mcr_py.utils import cache


class Buffering(Enum):
    """
    Enum class for buffering options.
    """

    UNBUFFERED = 0
    BUFFERED = 1


def convert_to_crs(
    geometry: shapely.geometry.base.BaseGeometry, crs: str, crs_target: str
) -> shapely.geometry.base.BaseGeometry:
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


@serde
@dataclass
class GeoMeta:
    """
    GeoMeta incorporates general geospatial information, including the boundary of the area of consideration.
    """

    crs: str
    crs_target: str
    boundary_wkt: str
    unbuffered_boundary_wkt: str
    buffer: int = 10000  # roughly 5km

    @staticmethod
    def create(boundary: Polygon, crs: str, crs_target: str, buffer: int = 10000) -> "GeoMeta":
        """
        Initialize the GeoMeta object with a boundary, source CRS, and target CRS.

        boundary: The polygon representing the boundary.
        crs: The source CRS as a string.
        crs_target: The target CRS as a string.
        """
        buffered_boundary = convert_to_crs(boundary, crs, crs_target)
        buffered_boundary = buffered_boundary.buffer(buffer)
        boundary_wkt = convert_to_crs(buffered_boundary, crs_target, crs).wkt
        return GeoMeta(
            crs,
            crs_target,
            boundary_wkt=boundary_wkt,
            unbuffered_boundary_wkt=boundary.wkt,
        )

    def hash_boundary(self) -> int:
        """
        Generate a hash string for the boundary geometry.

        :returns: A hash string representing the boundary.
        """
        return cache.hash_str(self.boundary_wkt)

    def get_bounding_box(
        self, buffering: Buffering = Buffering.BUFFERED
    ) -> tuple[float, float, float, float]:
        """
        Get the bounding box of the boundary.

        use_buffer: Whether to use the buffered boundary or the unbuffered boundary.

        :returns: A tuple representing the bounding box (min_x, min_y, max_x, max_y).
        """
        if buffering == Buffering.BUFFERED:
            return shapely.from_wkt(self.boundary_wkt).bounds
        else:
            return shapely.from_wkt(self.unbuffered_boundary_wkt).bounds

    def get_convex_hull_coord_list(
        self, buffering: Buffering = Buffering.BUFFERED
    ) -> list[tuple[float, float]]:
        """
        Get the coordinates of the boundary as a list.

        use_buffer: Whether to use the buffered boundary or the unbuffered boundary.

        :returns: A list of tuples representing the coordinates of the boundary.
        """
        if buffering == Buffering.BUFFERED:
            return list(shapely.from_wkt(self.boundary_wkt).convex_hull.boundary.coords)
        else:
            return list(
                shapely.from_wkt(self.unbuffered_boundary_wkt).convex_hull.boundary.coords
            )

    def get_bounding_box_as_coord_list(
        self, buffering: Buffering = Buffering.BUFFERED
    ) -> List[Tuple[float, float]]:
        """
        Get the bounding box as a list of coordinates.

        use_buffer: Whether to use the buffered boundary or the unbuffered boundary.

        :returns: A list of tuples representing the corners of the bounding box.
        """
        bounding_box = self.get_bounding_box(buffering=buffering)
        return [
            (bounding_box[0], bounding_box[1]),
            (bounding_box[0], bounding_box[3]),
            (bounding_box[2], bounding_box[3]),
            (bounding_box[2], bounding_box[1]),
        ]

    @staticmethod
    def load(path: pathlib.Path) -> "GeoMeta":
        """
        Load a GeoMeta object from a pickle file.

        path: The path to the pickle file containing the GeoMeta object.

        :returns: The loaded GeoMeta object.
        :raises ValueError: If the loaded object is not a GeoMeta.
        """
        with open(path, "r") as f:
            read_ = f.read()
            loaded = from_json(GeoMeta, read_)
            return loaded

    def set_residential_area(self, residential_area: MultiPolygon) -> None:
        """
        Set the residential area for the GeoMeta object.

        residential_area: A MultiPolygon representing the residential area.
        """
        self.residential_area = residential_area

    def save(self, path: pathlib.Path) -> None:
        """
        Save the GeoMeta object to a json file.

        path: The path where the GeoMeta object will be saved.
        """
        path.parent.mkdir(parents=True, exist_ok=True)

        with open(path, "w") as f:
            f.write(to_json(self))

    def crop_gdf(
        self, locations: pl.DataFrame, buffering: Buffering = Buffering.BUFFERED
    ) -> pl.DataFrame:
        """
        Crop a GeoDataFrame to the boundaries defined in the GeoMeta object.

        locations: The input GeoDataFrame to be cropped.
        use_buffer: Whether to use the buffered boundary or the unbuffered boundary.

        :returns: A cropped GeoDataFrame containing only the locations within the boundary.
        """
        boundary = self.unbuffered_boundary_wkt
        if buffering == Buffering.BUFFERED:
            boundary = self.boundary_wkt
        locations = locations.filter(
            st.geom("geometry").st.within(st.from_wkt(pl.lit(boundary)))
        )

        return locations

    def crop_df(
        self,
        locations: pl.DataFrame,
        lat_col: str,
        lon_col: str,
        buffering: Buffering = Buffering.BUFFERED,
    ) -> pl.DataFrame:
        """
        Crop a DataFrame of locations to the boundaries defined in the GeoMeta object.

        locations: The input DataFrame to be cropped.
        lat_col: The name of the latitude column.
        lon_col: The name of the longitude column.
        use_buffer: Whether to use the buffered boundary or the unbuffered boundary.

        :returns: A cropped DataFrame containing only the locations within the boundary.
        """
        boundary = self.unbuffered_boundary_wkt
        if buffering == Buffering.BUFFERED:
            boundary = self.boundary_wkt

        locations = locations.filter(
            st.point(pl.concat_arr(lon_col, lat_col)).st.within(st.from_wkt(pl.lit(boundary)))
        )

        return locations

    def get_center_lat_lon(self) -> tuple[float, float]:
        """
        Get the latitude and longitude of the centroid of the boundary.

        :returns: A tuple containing the latitude and longitude of the centroid.
        """
        lon, lat = shapely.from_wkt(self.boundary_wkt).centroid.coords[0]
        return lat, lon

    def add_to_folium_map(self, m: folium.Map) -> folium.Map:
        """
        Add the boundary and residential area to a Folium map.

        m: The Folium map to which the geometries will be added.

        :returns: The updated Folium map with added geometries.
        """
        folium.GeoJson(shapely.from_wkt(self.boundary_wkt)).add_to(m)
        folium.GeoJson(shapely.from_wkt(self.unbuffered_boundary_wkt)).add_to(m)

        return m
