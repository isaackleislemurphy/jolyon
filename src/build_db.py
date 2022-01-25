import sys

sys.path.append("/Users/IKleisle/F1")

from jolyon.src.etl import fetch_parse_data
from jolyon.src.utils import insert_table, config_db
from jolyon.src.db_configs import (
    LAP_TIME_CONFIGS,
    LAP_TIME_NAMES,
    DRIVER_CONFIGS,
    DRIVER_NAMES,
    RESULT_CONFIGS,
    RESULT_NAMES,
    RACE_CONFIGS,
    RACE_NAMES,
    CONSTRUCTOR_CONFIGS,
    CONSTRUCTOR_NAMES,
    LAP_TIME_DELTA_CONFIGS,
    LAP_TIME_DELTA_NAMES,
    QUALIFYING_CONFIGS,
    QUALIFYING_NAMES,
    QUALIFYING_SUMMARY_CONFIGS,
    QUALIFYING_SUMMARY_NAMES,
)


def make_db_from_csv():
    """
    Remakes the JOLYON database from scratch
    """
    # get data
    data = fetch_parse_data()
    # clean out and re-stand-up db
    config_db()
    # inserts
    insert_table(
        data["lap_times_full"][LAP_TIME_NAMES], "lap_times", LAP_TIME_CONFIGS,
    )
    insert_table(data["drivers"][DRIVER_NAMES], "drivers", DRIVER_CONFIGS)
    insert_table(data["races"][RACE_NAMES], "races", RACE_CONFIGS)
    insert_table(
        data["constructors"][CONSTRUCTOR_NAMES], "constructors", CONSTRUCTOR_CONFIGS,
    )
    insert_table(data["results"][RESULT_NAMES], "results", RESULT_CONFIGS)
    insert_table(
        data["lap_time_deltas"][LAP_TIME_DELTA_NAMES],
        "lap_time_deltas",
        LAP_TIME_DELTA_CONFIGS,
    )
    insert_table(
        data["qualifying"][QUALIFYING_NAMES], "qualifying", QUALIFYING_CONFIGS,
    )
    insert_table(
        data["qualifying_summary"][QUALIFYING_SUMMARY_NAMES],
        "qualifying_summary",
        QUALIFYING_SUMMARY_CONFIGS,
    )


if __name__ == "__main__":
    make_db_from_csv()
