import sqlite3


def map_table(conn):
    cursor = conn.cursor()
    writing_cursor = conn.cursor()
    # drop the table and index
    query = "DROP TABLE IF EXISTS Map;"
    cursor.execute(query)
    print("Dropped table Map")
    query = "DROP INDEX IF EXISTS MapIndex;"
    cursor.execute(query)
    print("Dropped Index MapIndex\n")

    # need to somehow include times for the table
    query = """CREATE TABLE Map (
        id INTEGER PRIMARY KEY NOT NULL,
        trip_id INTEGER NOT NULL,
        trip_headsign TEXT NOT NULL, --i.e. direction
        route_id TEXT NOT NULL,
        stop_name TEXT NOT NULL,
        stop_id INTEGER NOT NULL,
        stop_seq INTEGER NOT NULL,
        direction_id INTEGER NOT NULL,
        arrival_time INTEGER NOT NULL -- testing if it works
    );
    """
    cursor.execute(query)
    print("Inserting table and adding data")

    # retrieve the necessary information from current database
    query = """SELECT Trips.trip_id, Trips.trip_headsign, Trips.route_id, Stops.stop_name, Stops.stop_id, StopsInfo.stop_seq, direction_id FROM (SELECT DISTINCT stop_name, route_id, trip_headsign, stop_seq FROM StopsInfo) AS StopsInfo JOIN Trips on Trips.trip_headsign = StopsInfo.trip_headsign and Trips.route_id = StopsInfo.route_id JOIN Stops on Stops.stop_name = StopsInfo.stop_name;
    """
    cursor.execute(query)
    chunk_size = 1000000
    i = 1
    while True:
        chunk = cursor.fetchmany(chunk_size)
        if chunk is None or len(chunk) == 0:
            break
        else:
            writing_cursor.execute("BEGIN TRANSACTION;")
            sql = "INSERT INTO Map (trip_id,trip_headsign,route_id,stop_name,stop_id,stop_seq,direction_id, arrival_time) VALUES (?,?,?,?,?,?,?,0);\n"
            print(f"Created chunk #{i}, executing query")
            writing_cursor.executemany(sql, chunk)
            conn.commit()
            i += 1
            # chunk = []
    print("Successfully inserted data in table Map")

    query = "CREATE INDEX MapIndex ON Map(trip_id,route_id,stop_id,direction_id);"
    print("Creating index for StopTimes on stopid and tripid")
    cursor.execute(query)
    print("Successfully created index for table StopTimes")
    cursor.close()


def main():
    conn = sqlite3.connect("./stm_info.db")
    conn.execute('PRAGMA encoding = "UTF-8"')
    map_table(conn)
    conn.close()


if __name__ == "__main__":
    main()
