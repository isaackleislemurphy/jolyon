# Jolyon

An F1 database, named after the driver-turned-BBC-FiveLive-commentator. All data courtesy of the ![Ergast API](http://ergast.com/mrd/). For the purposes of me goofing around.

Relevant tables include (see `src/db_configs.py` for precise architecture):

* `lap_times_full`: Lap time, race time, position, stint, and outlap/inlap bools.
* `drivers`: Basic info/metadata about drivers.
* `races`: Basic info/metadata about races
* `constructors`: Basic info/metadata about constructors.
* `results`: Race-by-race box scores.
* `lap_time_deltas`: Driver-to-driver deltas by race and lap.
* `qualifying`: Qualifying (best) times by race, driver, and knockout round.
* `qualifying_summary`: Q1/Q2/Q3 cut times, purple times, and 107 times by race.

The API's given me trouble of late, so easiest to download `.zip` and move to `/data` folder.

To build the database, make sure you have installed `etc/requirements.txt`. Then run `build_db.py`, and a SQLite3 `.db` file will be built. M
