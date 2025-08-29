import polars as pl
from mcr_py.osm.osm import (
    list_column_to_osm_nodes,
)  # Adjust the import based on your module structure


def test_list_column_to_osm_nodes() -> None:
    # Create a sample osm_nodes_df DataFrame
    osm_nodes_df = pl.DataFrame({"id": [1, 2, 3], "some_other_column": ["a", "b", "c"]})

    # Create a sample df DataFrame
    df = pl.DataFrame(
        {
            "nearest_osm_node": [1, 1, 2, 3, 3, 3],
            "value_column": ["val1", "val2", "val3", "val4", "val5", "val6"],
        }
    )

    # Call the function to test
    result = list_column_to_osm_nodes(osm_nodes_df, df, "value_column")

    # Expected result DataFrame
    expected_result = pl.DataFrame(
        {
            "nearest_osm_node": [1, 2, 3],
            "some_other_column": ["a", "b", "c"],
            "value_column": [["val1", "val2"], ["val3"], ["val4", "val5", "val6"]],
        }
    )
    # Verify the result
    assert (
        result.get_column("value_column") == expected_result.get_column("value_column")
    ).all()
