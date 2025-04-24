import polars as pl
import numpy as np
import rustworkx as rx
import os

from mcr_py.utils.logger import rlog


def create_rx_graph(
    nodes: pl.DataFrame, edges: pl.DataFrame, network_type: str
) -> rx.PyDiGraph:
    # network_type only parameter
    # Can use rustworx directly
    # Need to check igraph vs rustworkx
    # Likely to be relevant
    graph = rx.PyDiGraph()

    nodes = nodes.with_columns(
        pl.Series(
            name="rx_node_id",
            values=np.array(graph.add_nodes_from(nodes.get_column("id").to_numpy())),
        )
    )
    edges = edges.join(
        nodes.select(pl.col("osm_id"), pl.col("rx_node_id").alias("source_rx_node_id")),
        left_on="source_osm",
    ).join(
        nodes.select(pl.col("osm_id"), pl.col("rx_node_id").alias("dest_rx_node_id")),
        left_on="dest_osm",
    )

    graph.add_nodes_from(
        zip(
            nodes["source_rx_node_id"].to_numpy(),
            nodes["dest_rx_node_id"].to_numpy(),
            nodes["length"].to_numpy(),
        )
    )

    # Flow:
    #  - generate directed edges (might need to duplicate direction based on network type)

    return graph


def crop_graph_to_largest_component(
    graph: rx.PyDiGraph, nodes: pl.DataFrame, edges: pl.DataFrame
) -> tuple[rx.PyDiGraph, pl.DataFrame, pl.DataFrame]:
    weakly_connected_components = rx.weakly_connected_components(graph)
    largest_component = max(weakly_connected_components, key=len)
    graph = graph.subgraph(list(largest_component))

    n_nodes_before, n_edges_before = len(nodes), len(edges)
    nodes = nodes.filter(pl.col("rx_node_id").is_in(largest_component))
    edges = edges.filter(
        (pl.col("source_rx_node_id").is_in(largest_component))
        & (pl.col("dest_rx_node_id").is_in(largest_component))
    )
    rlog.debug(
        f"Removed {n_nodes_before - len(nodes)} nodes and "
        + f" {n_edges_before - len(edges)} edges from OSM network to ensure"
        + f" connectivity ({(n_nodes_before - len(nodes)) / n_nodes_before * 100:.2f}%)"
    )
    return graph, nodes, edges


def shortest_paths(graph: rx.PyDiGraph, num_threads=4) -> rx.AllPairsPathLengthMapping:
    os.environ["RAYON_NUM_THREADS"] = str(num_threads)
    return rx.all_pairs_bellman_ford_path_lengths(
        graph, edge_cost_fn=lambda length: length
    )


def add_nearest_node_to_stops(
    stops_df: pl.DataFrame, nx_graph: rx.PyDiGraph
) -> pl.DataFrame:
    # osmnx nearest_nodes add -> uses some nx feature
    # Uses a ckdtree internally
    # might be good to also do this in osmtools, as the feature is implemented there anyways
    # -> give a polars dataframe, add columns nearest_node and nearest_node_dist
    # -> Later, might be important to do this after osm node id reset or before, depending on how the logic is easier
    nodes, dists = ox.nearest_nodes(
        nx_graph,
        stops_df.stop_lon.astype(float),  # TODO: improve this
        stops_df.stop_lat.astype(float),
        return_dist=True,
    )  # type: ignore
    stops_df["nearest_node"] = nodes
    stops_df["nearest_node_dist"] = dists
    return stops_df
