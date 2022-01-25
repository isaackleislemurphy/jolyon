"""
Setup/insert utils for standing up the JOLYON database
"""
import os
import os.path
import sqlite3 as sql
import numpy as np
import pandas as pd
import math
from jolyon.src.db_configs import DBNAME

import os.path

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, DBNAME)


def config_db():
    """Clears any existing Jolyon database, and starts it anew"""
    if DBNAME in os.listdir():
        os.remove(DBNAME)
    con = sql.connect(DBNAME)
    con.close()


connect = lambda: sql.connect(DBNAME)


def determine_null(item):
    """
    Helper to determine whether an item should be passed to SQLite
    as a null, as a text/varchar, or as-is.
    """
    if isinstance(item, str):
        return item if item in ("NULL", "null", "Null") else f"'{item}'"
    elif np.isnan(item) or math.isnan(item) or item is None:
        return "NULL"
    else:
        return str(item)


def insert_table(data, table_name, table_configs, dbname=DBNAME):
    """
    Initializes the lap_times table
    """
    data_db = data.to_dict("split")
    con = sql.connect(DBNAME)
    cur = con.cursor()
    try:
        cur.execute(
            f"""CREATE TABLE {table_name} (
                {", ".join([f"{k} {v}" for (k, v) in table_configs.items()])}
                )"""
        )
    except sql.OperationalError:
        pass
    for row in data_db["data"]:
        try:
            replace_vals = ", ".join([determine_null(item) for item in row])
            cur.execute(f"INSERT INTO {table_name} VALUES ({replace_vals})")
        except Exception as err:
            print(err)
            return row
    con.commit()
    con.close()


def query(statement, dbname=DBNAME):
    """
    Function to hit JOLYON database.

    Args:
        statement : str
            Your SQL statement.
    Returns : pd.DataFrame
        Your SQL query, in pandas form.
    """
    result = []
    con = sql.connect(dbname)
    cur = con.cursor()
    cur_job = cur.execute(statement)
    names = list(map(lambda x: x[0], cur.description))
    for row in cur_job:
        result.append(row)
    con.close()
    return pd.DataFrame(result, columns=names)
