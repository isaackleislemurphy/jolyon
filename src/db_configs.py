"""
Basic constants and table dicts for JOLYON database.
"""

DBNAME = 'jolyon.db'

DATA_FILES = [
 'circuits.csv',
 'status.csv',
 'lap_times.csv',
 'drivers.csv',
 'races.csv',
 'constructors.csv',
 'constructor_standings.csv',
 'qualifying.csv',
 'driver_standings.csv',
 'constructor_results.csv',
 'pit_stops.csv',
 'seasons.csv',
 'results.csv'
]


LAP_TIME_CONFIGS = {
    "race_id": "SMALLINT",
    "driver_id": "SMALLINT",
    "lap": "TINYINT",
    "race_laps": "TINYINT",
    "pct_complete": "REAL",
    "seconds": "REAL",
    "total_seconds": "REAL",
    "position": "TINYINT",
    "stops": "TINYINT",
    "stint": "TINYINT",
    "is_inlap": "TINYINT",
    "is_outlap": "TINYINT"
}

LAP_TIME_NAMES = [
    "raceId",
    "driverId",
    "lap",
    "race_laps",
    "pct_complete",
    "seconds",
    "total_seconds",
    "position",
    "stops",
    "stint",
    "is_inlap",
    "is_outlap"
]


DRIVER_CONFIGS = {
    "driver_id": "SMALLINT",
    "number": "TINYINT",
    "code": "text",
    "name": "text",
    "birth_date": "date",
    "nationality": "text"
}

DRIVER_NAMES = [
    "driverId",
    "number",
    "code", 
    "name",
    "dob",
    "nationality"
]

CONSTRUCTOR_CONFIGS = {
    "constructor_id": "SMALLINT",
    "name": "TEXT",
    "nationality": "TEXT"
}
CONSTRUCTOR_NAMES = [
    "constructorId",
    "name",
    "nationality"
]

RACE_CONFIGS = {
    "race_id": "SMALLINT",
    "year": "SMALLINT",
    "round": "SMALLINT",
    "circuit_id": "SMALLINT",
    "name": "TEXT",
    "date": "DATE"
}

RACE_NAMES = [
    "raceId",
    "year",
    "round",
    "circuitId",
    "name",
    "date"
]


RESULT_CONFIGS = {
    "year": "SMALLINT",
    "race_id": "SMALLINT",
    "driver_id": "SMALLINT",
    "constructor_id": "SMALLINT",
    "grid": "TINYINT",
    "position": "TINYINT",
    "classification": "TINYINT",
    "laps": "TINYINT",
    "pct_complete": "REAL",
    "not_on_grid": "TINYINT",
    "pitlane_start": "TINYINT",
    "points": "REAL",
    "status": "TEXT",
    "finished_running": "TINYINT"
    
}

RESULT_NAMES = [
    "year",
    "raceId",
    "driverId",
    "constructorId",
    "grid",
    "position",
    "classification",
    "laps",
    "pct_complete",
    "not_on_grid",
    "pitlane_start",
    "points",
    "status",
    "finished_running"
]


LAP_TIME_DELTA_CONFIGS = {
    "race_id": "SMALLINT",
    "lap": "TINYINT",
    "position_ahead": "TINYINT",
    "position_behind": "TINYINT",
    "driver_id_ahead": "SMALLINT",
    "driver_id_behind": "SMALLINT",
    "lap_time_delta": "REAL",
    "race_time_delta": "REAL",
    "is_lap_down_ahead": "TINYINT",
    "is_retired_ahead": "TINYINT",
    "is_lap_down_behind": "TINYINT",
    "is_retired_behind": "TINYINT"
    
}

LAP_TIME_DELTA_NAMES = [
    "raceId",
    "lap",
    "position_ahead",
    "position_behind",
    "driverId_ahead",
    "driverId_behind",
    "lap_time_delta",
    "race_time_delta",
    "is_lap_down_ahead",
    "is_retired_ahead",
    "is_lap_down_behind",
    "is_retired_behind"
]


QUALIFYING_CONFIGS = {
    "race_id": "SMALLINT",
    "driver_id": "SMALLINT",
    "position": "TINYINT",
    "q1": "REAL",
    "q2": "REAL",
    "q3": "REAL",
    "rank_q1": "TINYINT",
    "rank_q2": "TINYINT",
    "rank_q3": "TINYINT",
}

QUALIFYING_NAMES = [
    "raceId",
    "driverId", 
    "position",
    "q1",
    "q2",
    "q3",
    "rank_q1",
    "rank_q2",
    "rank_q3"
]


QUALIFYING_SUMMARY_CONFIGS = {
    "race_id": "TINYINT",
    "q1_escape_time": "REAL",
    "q2_escape_time": "REAL",
    "pole_time": "REAL",
    "best_time": "REAL",
    "time_107": "REAL"
}

QUALIFYING_SUMMARY_NAMES = [
    "raceId",
    "q1_escape_time",
    "q2_escape_time",
    "pole_time",
    "best_time",
    "time_107"
]