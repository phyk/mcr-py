import typing

import polars as pl

import mcr_py.utils.strtime

PROFILE_MAX_TIME = mcr_py.utils.strtime.str_time_to_seconds("48:00:00")


def profile_calculation_worker(
    poi_types: list[str], args: tuple[str, pl.DataFrame]
) -> typing.Union[None, tuple[str, list[tuple[int, int]]]]:
    name, group = args
    profile = calculate_profile_for_group(group, poi_types)
    if profile is not None:
        return name, profile
    return None


def calculate_profile_for_group(
    group: pl.DataFrame, poi_types: list[str]
) -> list[tuple[int, int]]:
    costs = group.get_column("cost").unique().sort().to_list()

    profile = []

    for cost in costs:
        labels_for_cost = group.filter(pl.col("cost") <= cost)

        labels_for_cost = labels_for_cost.lazy().sort(by=["time"], descending=False)
        for poi_type in poi_types:
            labels_for_cost = labels_for_cost.with_columns(
                pl.col(poi_type).cast(pl.Int32).cum_sum()
            )
        labels_for_cost = labels_for_cost.filter(pl.all_horizontal(pl.col(*poi_types) > 0))
        curr_time = labels_for_cost.collect().get_column("time").min()
        if not isinstance(curr_time, int):
            error_msg = "Time should always be an integer"
            raise ValueError(error_msg)
        if curr_time > PROFILE_MAX_TIME:
            start_id_hex = group.get_column("start_id_hex").first()
            error_msg = f"Time limit exceeded for hex id {start_id_hex} at cost {cost}"
            raise ValueError(error_msg)

        profile.append((cost, curr_time))

    return profile


def build_profiles_df(
    profiles: dict[str, list[tuple[int, int]]], start_time: int
) -> pl.DataFrame:
    profiles_to_list = [
        (hex_id, cost, time) for hex_id, profile in profiles.items() for cost, time in profile
    ]
    profiles_df = pl.DataFrame(
        profiles_to_list,
        schema={"hex_id": pl.String, "cost": pl.Int64, "time": pl.Int64},
        orient="row",
    ).with_columns(pl.col("time") - pl.lit(start_time))

    profiles_df = profiles_df.pivot(index="hex_id", on="cost", values="time")

    rename_columns = [f"cost_{c}" for c in profiles_df.columns if c != "hex_id"]
    profiles_df.columns = ["hex_id"] + rename_columns

    rename_columns.sort()
    profiles_df = profiles_df.select(["hex_id"] + rename_columns)

    profiles_df = fill_columns_by_left(profiles_df)

    return profiles_df


def fill_columns_by_left(profiles_df: pl.DataFrame) -> pl.DataFrame:
    if "cost_0" not in profiles_df.columns:
        # fill first cost in case it is not possible to reach without any cost (e.g. car, that can't stop for some time)
        profiles_df = profiles_df.with_columns(cost_0=pl.lit(float("inf")))
        profiles_df = profiles_df.select(
            ["hex_id", "cost_0"]
            + [
                column
                for column in profiles_df.columns
                if column != "hex_id" and column != "cost_0"
            ]
        )
    else:
        profiles_df = profiles_df.with_columns(pl.col("cost_0").fill_null(float("inf")))

    profiles_df = (
        profiles_df.transpose(include_header=True, header_name="cost", column_names="hex_id")
        .fill_null(strategy="forward")
        .transpose(include_header=True, header_name="hex_id", column_names="cost")
    )
    return profiles_df


def add_any_column_is_different_column(profiles_df: pl.DataFrame) -> pl.DataFrame:
    cost_rows = [c for c in profiles_df.columns if c.startswith("cost_")]
    cost_rows.sort()
    profiles_df = profiles_df.with_columns(
        any_column_different=pl.any_horizontal(pl.col(cost_rows[0]) != pl.col(*cost_rows[1:]))
    )
    return profiles_df


def add_optimum_column(profiles_df: pl.DataFrame) -> pl.DataFrame:
    cost_rows = [c for c in profiles_df.columns if c.startswith("cost_")]
    cost_values = [int(c.replace("cost_", "")) for c in cost_rows]

    profiles_df = profiles_df.with_columns(
        pl.concat_list(pl.col(cost_rows)).alias("all_costs")
    ).with_columns(
        pl.col("all_costs").list.min().alias("optimal"),
        pl.col("all_costs")
        .list.arg_min()
        .replace(dict(enumerate(cost_values)))
        .alias("required_cost_for_optimal"),
    )
    return profiles_df
