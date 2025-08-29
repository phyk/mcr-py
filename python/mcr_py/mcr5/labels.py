import pathlib

import polars as pl


def read_labels_for_nodes(directory: str, nodes: pl.DataFrame) -> pl.DataFrame:
    base_path = pathlib.Path(directory)

    labels = (
        pl.scan_ipc(base_path.name + "*.feather", include_file_paths="start_id_hex")
        .join(nodes.lazy(), on="osm_node_id", how="inner")
        .with_columns(
            pl.col("start_id_hex").str.split("/").list.last().str.split(".").list.first(),
            pl.col("osm_node_id").alias("target_id_osm"),
        )
        .select("start_id_hex", "target_id_osm")
    )

    return labels.collect()
