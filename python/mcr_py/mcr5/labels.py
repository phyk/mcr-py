import polars as pl


def read_labels_for_nodes(directory: str, nodes: pl.LazyFrame) -> pl.LazyFrame:
    labels = (
        pl.scan_ipc(directory + "/*.feather", include_file_paths="start_id_hex")
        .join(nodes.lazy(), on="osm_node_id", how="inner")
        .with_columns(
            pl.col("start_id_hex").str.split("/").list.last().str.split(".").list.first(),
            pl.col("osm_node_id").alias("target_id_osm"),
        )
    )

    return labels
