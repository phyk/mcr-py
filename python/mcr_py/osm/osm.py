import polars as pl


def list_column_to_osm_nodes(
    osm_nodes_df: pl.DataFrame, df: pl.DataFrame, column: str
) -> pl.DataFrame:
    """
    Assigns each entry in df to a node in osm_nodes_df and lists all the values of column
    for each node.

    Args:
        df: The dataframe to assign to osm_nodes_df. Must contain columns "nearest_osm_node" and column.
        osm_nodes_df: The dataframe to assign df to. The index must be the osm node ids.
    """
    grouped = (
        df.select(["nearest_osm_node", column])
        .group_by("nearest_osm_node", maintain_order=True)
        .agg(pl.col(column).unique(maintain_order=True))
    )
    # drop column if it already exists to make this function idempotent
    if column in osm_nodes_df.columns:
        osm_nodes_df = osm_nodes_df.drop(column)
    osm_nodes_df = osm_nodes_df.join(
        grouped, left_on="id", right_on="nearest_osm_node", how="left"
    )
    osm_nodes_df = osm_nodes_df.with_columns(pl.col(column).fill_null(pl.lit([])))
    return osm_nodes_df
