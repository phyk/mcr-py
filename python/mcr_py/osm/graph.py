import os
from typing import Tuple

import numpy as np
import polars as pl
import rustworkx as rx

from mcr_py.utils.logger import rlog


def create_rx_graph(
    nodes: pl.DataFrame, edges: pl.DataFrame
) -> Tuple[pl.DataFrame, pl.DataFrame, rx.PyDiGraph]:
    """
    Creates a directed graph from the given nodes and edges DataFrames.

    :param nodes: pl.DataFrame - A DataFrame containing node information with an 'osm_id' column.
    :param edges: pl.DataFrame - A DataFrame containing edge information with 'source_osm', 'dest_osm', and 'length' columns.
    :returns: tuple[pl.DataFrame, pl.DataFrame, rx.PyDiGraph] - A tuple containing the updated nodes DataFrame, updated edges DataFrame, and the constructed directed graph.
    """
    graph = rx.PyDiGraph()

    nodes = nodes.with_columns(
        pl.Series(
            name="rx_node_id",
            values=np.array(graph.add_nodes_from(nodes.get_column("osm_id").to_numpy())),
        )
    )
    edges = edges.join(
        nodes.select(pl.col("osm_id"), pl.col("rx_node_id").alias("source_rx_node_id")),
        left_on="source_osm",
        right_on="osm_id",
    ).join(
        nodes.select(pl.col("osm_id"), pl.col("rx_node_id").alias("dest_rx_node_id")),
        left_on="dest_osm",
        right_on="osm_id",
    )

    graph.add_edges_from(
        zip(
            edges["source_rx_node_id"].to_numpy(),
            edges["dest_rx_node_id"].to_numpy(),
            edges["length"].to_numpy(),
            strict=False,
        )
    )
    return (nodes, edges, graph)


def crop_graph_to_largest_component(
    graph: rx.PyDiGraph, nodes: pl.DataFrame, edges: pl.DataFrame
) -> tuple[pl.DataFrame, pl.DataFrame, rx.PyDiGraph]:
    """
    Crops the input graph to its largest weakly connected component.

    :param graph: rx.PyDiGraph - The directed graph to be cropped.
    :param nodes: pl.DataFrame - A DataFrame containing node information with 'rx_node_id' column.
    :param edges: pl.DataFrame - A DataFrame containing edge information with 'source_rx_node_id' and 'dest_rx_node_id' columns.
    :returns: tuple[rx.PyDiGraph, pl.DataFrame, pl.DataFrame] - A tuple containing the cropped graph, filtered nodes DataFrame, and filtered edges DataFrame.
    """
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
    return nodes, edges, graph


def shortest_paths(graph: rx.PyDiGraph, num_threads: int = 4) -> rx.AllPairsPathLengthMapping:
    """
    Computes shortest paths for all pairs of nodes in the directed graph using the Bellman-Ford algorithm.

    :param graph: rx.PyDiGraph - The directed graph for which to compute shortest paths.
    :param num_threads: int - The number of threads to use for parallel processing (default is 4).
    :returns: rx.AllPairsPathLengthMapping - A mapping of shortest path lengths between all pairs of nodes.
    """
    os.environ["RAYON_NUM_THREADS"] = str(num_threads)
    return rx.all_pairs_bellman_ford_path_lengths(graph, edge_cost_fn=lambda length: length)
