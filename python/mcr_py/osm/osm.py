import polars as pl
from mcr_py.utils.logger import rlog


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
    column_names = [f"{column}_{value}" for value in df.get_column(column).unique()]
    rlog.debug("Found {} unique values in {}".format(len(column_names), column))
    df_dummied = (
        df.to_dummies(column)
        .select(["nearest_osm_node"] + column_names)
        .group_by("nearest_osm_node")
        .sum()
        .select(
            "nearest_osm_node",
            pl.concat_arr(pl.col(column_names).cast(pl.Boolean)).alias(column),
        )
    )
    # drop column if it already exists to make this function idempotent
    if column in osm_nodes_df.columns:
        osm_nodes_df = osm_nodes_df.drop(column)
    osm_nodes_df = osm_nodes_df.join(
        df_dummied, left_on="osm_id", right_on="nearest_osm_node", how="left"
    )
    osm_nodes_df = osm_nodes_df.with_columns(
        pl.col(column).fill_null(pl.lit([False for _ in column_names]))
    )
    return osm_nodes_df
