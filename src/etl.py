"""
ETL helpers for JOLYON database and its various tables.
"""
import os
import math
import numpy as np
import pandas as pd
from jolyon.src.db_configs import DATA_FILES


def fetch_data_local(filepath=""):
    """
    Compiles an entire CSV dump from Ergast into a dictionary of relevant
    datasets/pd.DataFrames.

    Args:
        filepath : str.
            A filepath to the CSV dump

    Returns : dict[<str : pd.DataFrame>]
        A dictionary of datasets in the Ergast CSV dump.
    """
    data = {}
    for item in DATA_FILES:
        item_df = pd.read_csv(f"{filepath}{item}")
        data[item.replace(".csv", "")] = item_df
    return data


def parse_pit_stops(pit_stop_df):
    """
    Parses the pit_stops.csv file provided by Ergast.

    Args:
        pit_stop_df : pd.DataFrame
            The dataframe containing raw pit_stops.csv
    Returns : pd.DataFrame
        A processed version of 'pit_stops.csv'
    """
    pit_stop_df_copy = pit_stop_df.copy()
    pit_stop_df_copy["seconds"] = np.divide(
        pit_stop_df_copy["milliseconds"].values, 1000.0
    )
    return pit_stop_df_copy[["raceId", "driverId", "stop", "lap", "seconds"]]


def parse_lap_times(data):
    """
    Performs basic wrangling/processing on the lap_times dataset.

    Args:
        data : dict
            A dictionary of data, as outputted by fetch_data_local()

    Returns : pd.DataFrame
        Wrangled lap time data.
    """
    lap_time_df = data["lap_times"].copy()
    lap_time_df["seconds"] = np.divide(lap_time_df["milliseconds"].values, 1000.0)
    lap_time_df = pd.merge(
        lap_time_df,
        data["results"].groupby("raceId", as_index=False)["laps"].max(),
        how="inner",
        on="raceId",
    ).rename(columns={"laps": "race_laps"})
    lap_time_df["pct_complete"] = np.divide(
        lap_time_df.lap.values, lap_time_df.race_laps.values
    )

    pit_stop_df = parse_pit_stops(data["pit_stops"])
    pit_stop_df_2 = pit_stop_df.copy()
    pit_stop_df_2["lap"] = pit_stop_df_2["lap"].values + 1

    combined_df = (
        lap_time_df.merge(
            pit_stop_df.drop(columns=["seconds"]),
            how="left",
            on=["raceId", "driverId", "lap"],
        )
        .merge(
            pit_stop_df_2[["raceId", "driverId", "lap", "stop"]],
            how="left",
            on=["raceId", "driverId", "lap"],
            suffixes=["_in", "_out"],
        )
        .sort_values(["raceId", "driverId", "lap"])
        .fillna(value={"stop_in": 0, "stop_out": 0})
    )

    combined_df["stops"] = combined_df.groupby(["raceId", "driverId"])[
        "stop_out"
    ].cummax()
    combined_df["total_seconds"] = combined_df.groupby(["raceId", "driverId"])[
        "seconds"
    ].cumsum()

    combined_df["stint"] = combined_df["stops"].values + 1
    combined_df["is_inlap"] = (combined_df.stop_in.values > 0).astype(int)
    combined_df["is_outlap"] = (combined_df.stop_out.values > 0).astype(int)
    return combined_df


def parse_quali_df(quali_df, fill_value=np.nan):
    """"""
    quali_df_new = (
        quali_df.copy()
        .reset_index(drop=True)
        .rename(columns={"q1": "q1_str", "q2": "q2_str", "q3": "q3_str"})
    )
    for q in ["q1", "q2", "q3"]:
        quali_secs = []
        for item in quali_df[q].values:
            try:
                lap_time_secs = 60.0 * int(item.split(":")[0]) + float(
                    item.split(":")[1]
                )
            except (TypeError, ValueError, AttributeError):
                lap_time_secs = fill_value
            quali_secs.append(lap_time_secs)
        quali_df_new[q] = quali_secs
    for q in ["q1", "q2", "q3"]:
        quali_df_new = quali_df_new.sort_values(["raceId", q])
        quali_df_new[f"rank_{q}"] = 1
        quali_df_new[f"rank_{q}"] = quali_df_new.groupby(["raceId"], as_index=False)[
            f"rank_{q}"
        ].cumsum()
    return quali_df_new


def parse_quali_summary(qualifying_df):
    """
    Provides a summary snapshot of qualifying, including:
    * Times necessary to have escaped Q1, Q2
    * Pole time
    * Best overall time

    Args:
        qualifying_df : pd.DataFrame
            A WRANGLED qualifying df, as outputted by parse_quali_df()
    Returns : pd.DataFrame
    """
    quali_df = qualifying_df.copy()
    quali_df["best_time"] = quali_df[["q1", "q2", "q3"]].apply(
        lambda x: min(x["q1"], x["q2"], x["q3"]), axis=1
    )
    q1_cut = (
        quali_df.query("rank_q1 >= 16")
        .sort_values(["raceId", "rank_q1"])
        .groupby(["raceId"], as_index=False)
        .head(1)
    )

    q2_cut = (
        quali_df.query("rank_q2 >= 11")
        .sort_values(["raceId", "rank_q2"])
        .groupby(["raceId"], as_index=False)
        .head(1)
    )

    pole_times = quali_df.query("rank_q3 == 1")

    best_times = quali_df.groupby(["raceId"], as_index=False)["best_time"].min()

    result = (
        q1_cut[["raceId", "q1"]]
        .merge(q2_cut[["raceId", "q2"]], how="left", on="raceId")
        .merge(pole_times[["raceId", "q3"]], how="left", on="raceId")
        .merge(best_times[["raceId", "best_time"]])
        .rename(
            columns={"q1": "q1_escape_time", "q2": "q2_escape_time", "q3": "pole_time"}
        )
    )
    result["time_107"] = 1.07 * result.pole_time.values
    return result


def parse_race_df():
    pass


def parse_driver_df(driver_df):
    driver_df_copy = driver_df.copy()
    driver_df_copy["number"] = np.array(
        [int(str(item)) if "N" not in item else np.nan for item in driver_df.number]
    )
    # driver_df_copy['dob'] = np.array([pd.Timestamp(item).date() for item in driver_df.dob])
    driver_df_copy["name"] = np.array(
        [
            f"{fore} {sur}".replace("'", "")
            for fore, sur in list(zip(driver_df.forename, driver_df.surname))
        ]
    )
    return driver_df_copy


def parse_result_df(data):
    result_df = data["results"].copy()
    result_df = pd.merge(
        result_df, data["races"][["year", "raceId"]], how="inner", on=["raceId"]
    )
    result_df = pd.merge(
        result_df,
        result_df.groupby("raceId", as_index=False)["laps"].max(),
        how="inner",
        on="raceId",
        suffixes=["", "_all"],
    )
    result_df["pct_complete"] = np.divide(
        result_df.laps.values, result_df.laps_all.values
    )

    result_df["not_on_grid"] = (result_df.grid.values == 0).astype(int)
    result_df["grid"] = np.array([100 if i == 0 else i for i in result_df.grid])
    result_df["classification"] = result_df[["positionText", "positionOrder"]].apply(
        lambda x: 100
        if x["positionText"] in ["W", "F"]
        else 99
        if x["positionText"] == "R"
        else int(x["positionOrder"]),
        axis=1,
    )
    result_df["position"] = result_df.positionOrder.values.astype(int)
    result_df["pitlane_start"] = (
        (result_df.grid.values == 100) * (result_df.not_on_grid.values == 1)
    ).astype(int)

    result_df = pd.merge(result_df, data["status"], how="left", on="statusId")
    result_df["finished_running"] = np.array(
        [int("Lap" in item or item == "Finished") for item in result_df.status.values]
    )
    # TODO: parse race time and fastest laps
    return result_df


def parse_lap_time_deltas(data):
    """"""

    results = (
        data["results"]
        .query("year >= 2005")
        .rename(columns={"position": "finish_position"})
    )
    lap_times = parse_lap_times(data)[
        ["raceId", "driverId", "position", "seconds", "total_seconds", "lap"]
    ]

    delta_df = (
        pd.merge(
            results[
                ["raceId", "driverId", "laps", "finished_running", "finish_position"]
            ],
            results[
                ["raceId", "driverId", "laps", "finished_running", "finish_position"]
            ],
            how="inner",
            on="raceId",
            suffixes=["_ahead", "_behind"],
        )
        .merge(
            lap_times[["raceId", "lap"]]
            .groupby(["raceId", "lap"], as_index=False)
            .head(1),
            how="left",
            on=["raceId"],
        )
        .merge(
            lap_times.rename(columns={"driverId": "driverId_ahead"}),
            how="left",
            on=["raceId", "driverId_ahead", "lap"],
        )
        .merge(
            lap_times.rename(columns={"driverId": "driverId_behind"}),
            how="left",
            on=["raceId", "driverId_behind", "lap"],
            suffixes=["_ahead", "_behind"],
        )
        .sort_values(
            [
                "raceId",
                "lap",
                "position_ahead",
                "position_behind",
                "total_seconds_ahead",
                "total_seconds_behind",
                "finish_position_ahead",
                "finish_position_behind",
            ]
        )
        .reset_index(drop=True)
    )

    # compute delta on the last lap
    delta_df["lap_time_delta"] = np.subtract(
        delta_df.seconds_ahead.values, delta_df.seconds_behind.values,
    )
    # compute overall race delta
    delta_df["race_time_delta"] = np.subtract(
        delta_df.total_seconds_ahead.values, delta_df.total_seconds_behind.values,
    )

    # compute laps down
    delta_df["is_lap_down_ahead"] = np.multiply(
        np.isnan(delta_df.seconds_ahead.values).astype(int),
        delta_df.finished_running_ahead.values,
    )

    delta_df["is_lap_down_behind"] = np.multiply(
        np.isnan(delta_df.seconds_behind.values).astype(int),
        delta_df.finished_running_behind.values,
    )

    # compute retirements
    delta_df["is_retired_ahead"] = np.multiply(
        np.isnan(delta_df.seconds_ahead.values).astype(int),
        1 - delta_df.is_lap_down_ahead.values,
    )

    delta_df["is_retired_behind"] = np.multiply(
        np.isnan(delta_df.seconds_behind.values).astype(int),
        1 - delta_df.is_lap_down_behind.values,
    )

    delta_df["ctr"] = 1

    delta_df["row_idx_behind"] = (
        delta_df.sort_values(
            [
                "raceId",
                "lap",
                "total_seconds_ahead",
                "finish_position_ahead",
                "total_seconds_behind",
                "finish_position_behind",
            ]
        )
        .groupby(["raceId", "lap", "driverId_ahead"])["ctr"]
        .cumsum()
    )

    delta_df["position_behind"] = delta_df[["position_behind", "row_idx_behind"]].apply(
        lambda x: x["row_idx_behind"]
        if np.isnan(x["position_behind"]) or math.isnan(x["position_behind"])
        else x["position_behind"],
        axis=1,
    )

    delta_df["row_idx_ahead"] = (
        delta_df.sort_values(
            [
                "raceId",
                "lap",
                "total_seconds_behind",
                "finish_position_behind",
                "total_seconds_ahead",
                "finish_position_ahead",
            ]
        )
        .groupby(["raceId", "lap", "driverId_behind"])["ctr"]
        .cumsum()
    )

    delta_df["position_ahead"] = delta_df[["position_ahead", "row_idx_ahead"]].apply(
        lambda x: x["row_idx_ahead"]
        if np.isnan(x["position_ahead"]) or math.isnan(x["position_ahead"])
        else x["position_ahead"],
        axis=1,
    )

    return delta_df


def fetch_parse_data():
    """
    Main ETL function: reads in and processes everything thus far
    """
    # TODO: switch print --> logger.info
    data = fetch_data_local("/Users/IKleisle/F1/data/")
    print("Data acquired.")

    data["qualifying"] = parse_quali_df(data["qualifying"])
    print("Quali data wrangled.")

    data["qualifying_summary"] = parse_quali_summary(data["qualifying"])
    print("Quali summary data wrangled.")

    data["drivers"] = parse_driver_df(data["drivers"])
    print("Driver metadata wrangled.")

    data["lap_times_full"] = parse_lap_times(data)
    print("Lap times wrangled.")

    data["results"] = parse_result_df(data)
    print("Results wrangled.")

    data["lap_time_deltas"] = parse_lap_time_deltas(data)
    print("Lap time deltas wrangled.")

    print("ETL complete!")
    return data
