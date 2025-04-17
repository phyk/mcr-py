import polars as pl

from mcr_py.utils.geometa import GeoMeta
from mcr_py.utils.logger import Timed, rlog


def get_graph_for_city_cropped_to_boundary(
    osm_reader, geo_meta: GeoMeta, network_type: str
):
    # TODO remove
    raise NotImplementedError()
    with Timed.info("Ensuring graph is connected"):
        nxgraph = mcr_py.osm.graph.create_nx_graph(
            osm_reader, nodes, edges, network_type
        )
        n_nodes_before = len(nodes)
        nxgraph, nodes, edges = graph.crop_graph_to_largest_component(
            nxgraph, nodes, edges
        )
        n_nodes_after = len(nodes)
        rlog.info(
            f"Removed {n_nodes_before - n_nodes_after} nodes from OSM network to ensure connectivity ({(n_nodes_before - n_nodes_after) / n_nodes_before * 100:.2f}%)"
        )

    return nodes, edges


def list_column_to_osm_nodes(
    osm_nodes_df: pl.DataFrame, df: pl.DataFrame, column: str
) -> pl.DataFrame:
    """
    Assigns each entry in df to a node in osm_nodes_df and lists all the values of column
    for each node.

    Args:
        df: The dataframe to assign to osm_nodes_df. Must contain columns "nearest_osm_node_id" and column.
        osm_nodes_df: The dataframe to assign df to. The index must be the osm node ids.
    """
    grouped = (
        df.select(["osm_id", column])
        .group_by("osm_id", maintain_order=True)
        .agg(pl.col(column).unique(maintain_order=True))
    )
    # drop column if it already exists to make this function idempotent
    if column in osm_nodes_df.columns:
        osm_nodes_df = osm_nodes_df.drop(column)
    osm_nodes_df = osm_nodes_df.join(
        grouped, left_on="osm_id", right_on="osm_id", how="left"
    )
    osm_nodes_df = osm_nodes_df.with_columns(pl.col(column).fill_null(pl.lit([])))
    return osm_nodes_df
