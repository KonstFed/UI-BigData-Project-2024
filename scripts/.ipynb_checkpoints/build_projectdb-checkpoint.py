"""
This module writes to postgresql from csv files
"""
import os

from pprint import pprint
import psycopg as psql


# Read password from secrets file
file = os.path.join("secrets", "psql.pass")
with open(file, "r", encoding="utf-8") as file:
    password = file.read().rstrip()


# build connection string
CONN_STRING = f"host=hadoop-04.uni.innopolis.ru port=5432 user=team20 \
    dbname=team20_projectdb password={password}"

# with psql.connect(conn_string) as conn:
# cur = conn.cursor()
# cur.execute("SELECT * FROM anime_temp LIMIT 1")
# print(cur.fetchall())

# Connect to the remote dbms
CONN = psql.connect(CONN_STRING)

# Create a cursor for executing psql commands
cur = CONN.cursor()
# Read the commands from the file and execute them.
with open(os.path.join("sql", "create_tables.sql"), encoding="utf-8") as file:
    content = file.read()
    cur.execute(content)
CONN.commit()
# Read the commands from the file and execute them.
with open(os.path.join("sql", "import_data.sql"), encoding="utf-8") as file:
    # We assume that the COPY commands in the file are ordered (1.depts, 2.emps)
    commands = file.readlines()
    with open(
        os.path.join("data", "anime-exploratory-dataset-2023.csv"), "r", encoding="utf-8"
    ) as anime:
        with cur.copy(commands[0]) as copy:
            copy.write(anime.read())

    with open(os.path.join("sql", "normalise.sql"), encoding="utf-8") as file:
        content = file.read()
        cur.execute(content)

    with open(
        os.path.join("data", "users-details-transformed-2023.csv"), "r", encoding="utf-8"
    ) as users:
        with cur.copy(commands[1]) as copy:
            copy.write(users.read())

    with open(
        os.path.join("data", "users-scores-transformed-2023.csv"), "r", encoding="utf-8"
    ) as scores:
        with cur.copy(commands[2]) as copy:
            copy.write(scores.read())

CONN.commit()

# If the sql statements are CRUD then you need to commit the change

pprint(CONN)
cur = CONN.cursor()
# Read the sql commands from the file
with open(os.path.join("sql", "test_database.sql"), encoding="utf-8") as file:
    commands = file.readlines()
    for command in commands:
        cur.execute(command)
        # Read all records and print them
        pprint(cur.fetchall())

CONN.close()
