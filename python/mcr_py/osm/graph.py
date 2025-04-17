import polars as pl
import networkx as nx
import rustworkx as rx

from mcr_py.utils.logger import rlog


def create_nx_graph(
    nodes: pl.DataFrame, edges: pl.DataFrame, network_type: str
) -> nx.Graph:
    # network_type only parameter
    # Can use rustworx directly
    # Need to check igraph vs rustworkx
    # Likely to be relevant

    graph: nx.Graph = osm.to_graph(
        nodes, edges, graph_type="networkx", network_type=network_type
    )  # type: ignore

    # Flow:
    #  - generate directed edges (might need to duplicate direction based on )
    # Insert directed edges and nodes as well as crs into MultiDiGraph
    # from networkx

    # Requirements for networkx
    # - implements weakl_connected_components to only select the largest connected component

    return graph


def crop_graph_to_largest_component(
    graph: nx.Graph, nodes: pl.DataFrame, edges: pl.DataFrame
) -> tuple[nx.Graph, pl.DataFrame, pl.DataFrame]:
    weakly_connected_components = nx.weakly_connected_components(graph)
    largest_component = max(weakly_connected_components, key=len)

    graph = graph.subgraph(largest_component).copy()

    n_nodes_before, n_edges_before = len(nodes), len(edges)
    nodes = nodes[nodes["id"].isin(graph.nodes)]  # type: ignore
    edges = edges[edges["u"].isin(graph.nodes) & edges["v"].isin(graph.nodes)]  # type: ignore
    rlog.debug(
        f"Removed {n_nodes_before - len(nodes)} nodes and "
        + f" {n_edges_before - len(edges)} edges from OSM network to ensure"
        + f" connectivity ({(n_nodes_before - len(nodes)) / n_nodes_before * 100:.2f}%)"
    )
    return graph, nodes, edges


def add_nearest_node_to_stops(
    stops_df: pl.DataFrame, nx_graph: nx.Graph
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
