import polars as pl
import rustworkx as rx
from mcr_py.osm.graph import (
    create_rx_graph,
    crop_graph_to_largest_component,
    shortest_paths,
)


# Sample data for tests
def create_sample_data():
    nodes = pl.DataFrame(
        {
            "osm_id": [1, 2, 3],
        }
    )

    edges = pl.DataFrame({"source_osm": [1, 2], "dest_osm": [2, 3], "length": [10, 15]})

    return nodes, edges


def test_create_rx_graph() -> None:
    nodes, edges = create_sample_data()
    nodes_df, edges_df, graph = create_rx_graph(nodes, edges)

    assert nodes_df.shape[0] == 3
    assert edges_df.shape[0] == 2
    assert len(graph) == 3
    assert len(graph.edges()) == 2


def test_create_rx_graph_empty() -> None:
    nodes = pl.DataFrame(schema={"osm_id": pl.Int64})
    edges = pl.DataFrame(
        schema={"source_osm": pl.Int64, "dest_osm": pl.Int64, "length": pl.Float64}
    )

    nodes_df, edges_df, graph = create_rx_graph(nodes, edges)

    assert len(nodes_df) == 0
    assert len(edges_df) == 0
    assert len(graph) == 0
    assert len(graph.edges()) == 0


def test_crop_graph_to_largest_component() -> None:
    nodes, edges = create_sample_data()
    nodes_df, edges_df, graph = create_rx_graph(nodes, edges)

    cropped_nodes, cropped_edges, graph = crop_graph_to_largest_component(
        graph, nodes_df, edges_df
    )

    assert len(cropped_nodes) == len(nodes_df)
    assert len(cropped_edges) == len(edges_df)


def test_shortest_paths() -> None:
    nodes, edges = create_sample_data()
    nodes_df, edges_df, graph = create_rx_graph(nodes, edges)

    path_lengths = shortest_paths(graph)

    assert len(path_lengths) == 3  # Should return path lengths for all nodes


def test_shortest_paths_empty_graph() -> None:
    graph = rx.PyDiGraph()

    path_lengths = shortest_paths(graph)

    assert len(path_lengths) == 0  # No paths in an empty graph
