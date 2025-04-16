import psycopg2
import argparse
import requests
import zipfile
import os
import sys


def download(url : str): #destination : str) -> None:
    """download and create respective directories"""
    zip_file = "data.zip"   #f"{destination}.zip"
    print(f"Downloading from {url}")
    response = requests.get(url)
    if response.status_code == 200:
        with open(zip_file, "wb") as file:
            file.write(response.content)
        print(f"Downloaded {url} to {zip_file} successfully")
        #if not os.path.exists(f"./{destination}"):
        #    os.makedirs(destination)
        with zipfile.ZipFile(zip_file, "r") as zip:
            zip.extractall(f"./")#{destination}")
        print(f"Extracted file from {zip_file}")
        os.remove(zip_file)
        print("Removed zip file")
    else:
        print(f"Failed to download {url}")


def init_database(username: str, password: str) -> None:
    """Initialise the data in the database associated to that agency"""
    try:
        conn = psycopg2.connect(
            database="bus2go",
            user=username,
            password=password
        )
        conn.set_client_encoding("UTF8")
        # to manually commit and allow usage of vacuuming
        conn.autocommit = True
        cursor = conn.cursor()
        cursor.execute("DROP TABLE IF EXISTS Calendar CASCADE;")
        cursor.execute("DROP TABLE IF EXISTS Forms CASCADE;")
        cursor.execute("DROP TABLE IF EXISTS Routes CASCADE;")
        cursor.execute("DROP TABLE IF EXISTS Shapes CASCADE;")
        cursor.execute("DROP TABLE IF EXISTS StopsInfo CASCADE;")
        cursor.execute("DROP TABLE IF EXISTS StopTimes CASCADE;")
        cursor.execute("DROP TABLE IF EXISTS Stops CASCADE;")
        cursor.execute("DROP TABLE IF EXISTS Trips CASCADE;")
        cursor.execute("DROP INDEX IF EXISTS TripsIndex CASCADE;")
        cursor.execute("DROP TABLE IF EXISTS Map CASCADE;")

        cursor.execute("DROP INDEX IF EXISTS StopsInfoIndex;")
        cursor.execute("DROP INDEX IF EXISTS StopTimesIndex;")
        cursor.execute("DROP INDEX IF EXISTS MapIndex;")

        calendar_table(conn)
        route_table(conn)
        forms_table(conn)
        shapes_table(conn)
        trips_table(conn)
        stops_table(conn)
        stop_times_table(conn)
        stops_info_table(conn)
        map_table(conn)

        conn.close()

        answer = input("Do you want to clean up the directory from txt files? (y/n) ")
        if answer == "yes" or "y":
            print("Cleaning up")
            os.remove("*.txt")
            print("Cleaned up")
        else:
            print("Not cleaning up")

    except psycopg2.OperationalError:
        print(f"The username {username} does not exist. Aborting the script.")
        sys.exit(1)


def calendar_table(conn):
    cursor = conn.cursor()

    sql = """CREATE TABLE Calendar (
    	id SERIAL PRIMARY KEY,
    	service_id TEXT UNIQUE NOT NULL,
        days VARCHAR(7) NOT NULL,
    	start_date INTEGER NOT NULL,
    	end_date INTEGER NOT NULL
    );"""
    cursor.execute(sql)
    print("Initialised table Calendar")

    print("Inserting in table Calendar and adding data")
    with open("calendar.txt", "r", encoding="utf-8") as file:
        file.readline()
        for line in file:
            tokens = line.replace("\n", "").replace("'", "''").split(",")
            #check all the possible letters
            days = ""
            if tokens[1] == "1":
                days += "m"
            if tokens[2] == "1":
                days += "t"
            if tokens[3] == "1":
                days += "w"
            if tokens[4] == "1":
                days += "y"
            if tokens[5] == "1":
                days += "f"
            if tokens[6] == "1":
                days += "s"
            if tokens[7] == "1":
                days += "d"
            sql = f"INSERT INTO Calendar (service_id,days,start_date,end_date) VALUES (%s,%s,%s,%s);"
            cursor.execute(sql, (tokens[0], days, tokens[8], tokens[9]))
            conn.commit()
    print("Successfully inserted table\n")
    cursor.close()


def forms_table(conn):
    cursor = conn.cursor()

    sql = """CREATE TABLE Forms(
    	id SERIAL PRIMARY KEY NOT NULL,
    	shape_id INTEGER UNIQUE NOT NULL
    );"""
    cursor.execute(sql)
    print("Initialised table Forms")

    print("Inserting in table Forms")
    queries = []
    with open("./shapes.txt", "r", encoding="utf-8") as file:
        file.readline()
        prev = ""
        for line in file:
            tokens = line.split(",")
            shape_id = tokens[0]
            if not shape_id == prev:
                queries.append((tokens[0],))
                prev = shape_id
        sql = "INSERT INTO Forms (shape_id) VALUES (%s);"
        #cursor.execute("BEGIN;")
        cursor.executemany(sql, queries)
        conn.commit()
    print("Successfully inserted table\n")

    cursor.close()


def route_table(conn):
    cursor = conn.cursor()

    sql = """CREATE TABLE Routes (
    	id SERIAL PRIMARY KEY NOT NULL,
    	route_id INTEGER UNIQUE NOT NULL,
    	route_long_name TEXT NOT NULL,
    	route_type INTEGER NOT NULL,
    	route_color TEXT NOT NULL
    );"""
    cursor.execute(sql)
    print("Initialised table routes")

    print("Inserting in table Route and adding data")
    with open("routes.txt", "r", encoding="utf-8") as file:
        file.readline()
        for line in file:
            tokens = line.replace("\n", "").replace("'", "''").split(",")
            sql = "INSERT INTO Routes (route_id,route_long_name,route_type,route_color) VALUES (%s,%s,%s,%s);"
            cursor.execute(sql, (tokens[0],tokens[3],tokens[4],tokens[6]))
            conn.commit()
    print("Successfully inserted table\n")

    cursor.close()


def shapes_table(conn):
    cursor = conn.cursor()
    sql = """CREATE TABLE Shapes(
    	id SERIAL PRIMARY KEY,
    	shape_id INTEGER NOT NULL REFERENCES Forms(shape_id),
    	lat REAL NOT NULL,
    	long REAL NOT NULL,
    	sequence INTEGER NOT NULL
    );"""
    cursor.execute(sql)
    print("Initialised table shapes")

    print("Inserting in table Shapes")
    queries = []
    with open("shapes.txt", "r", encoding="utf-8") as file:
        file.readline()
        for line in file:
            tokens = line.split(",")
            queries.append((tokens[0], tokens[1], tokens[2], tokens[3]))
        sql = "INSERT INTO Shapes (shape_id,lat,long,sequence) VALUES (%s,%s,%s,%s);"
        #cursor.execute("BEGIN;")
        cursor.executemany(sql, queries)
        conn.commit()
    print("Successfully inserted table\n")
    cursor.close()


def stop_times_table(conn):
    cursor = conn.cursor()

    # init the table
    query = """CREATE TABLE StopTimes (
    	id SERIAL PRIMARY KEY,
    	trip_id INTEGER NOT NULL,
    	arrival_time TEXT NOT NULL,
    	departure_time TEXT NOT NULL,
    	stop_id TEXT NOT NULL REFERENCES Stops(stop_id),
    	stop_seq INTEGER NOT NULL
    );
    """
    cursor.execute(query)
    print("Initialised tmp table StopTimes")
    print("Inserting table and adding data")

    with open("stop_times.txt", "r", encoding="utf-8") as file:
        cursor.copy_expert("COPY StopTimes(trip_id, arrival_time, departure_time, stop_id, stop_seq) FROM STDIN CSV HEADER;", file)
    conn.commit()

    query = "CREATE INDEX StopTimesIndex ON StopTimes(stop_id,trip_id);"
    print("Creating index for StopTimes on stopid and tripid")
    cursor.execute(query)
    print("Successfully created index for table StopTimes\n")
    cursor.close()


def stops_table(conn):
    cursor = conn.cursor()

    sql = """CREATE TABLE Stops (
    	id SERIAL PRIMARY KEY NOT NULL,
    	stop_id TEXT UNIQUE NOT NULL,
    	stop_code INTEGER NOT NULL,
    	stop_name TEXT NOT NULL,
    	lat REAL NOT NULL,
    	long REAL NOT NULL,
    	wheelchair INTEGER NOT NULL
    );"""
    cursor.execute(sql)
    print("Initialised table stops")

    print("Inserting in table Stops")
    chunk = []
    with open("stops.txt", "r", encoding="utf-8") as file:
        file.readline()
        for line in file:
            tokens = line.split(",")
            chunk.append((tokens[0], tokens[1], tokens[2], tokens[3], tokens[4], tokens[8]))
        sql = "INSERT INTO Stops (stop_id,stop_code,stop_name,lat,long,wheelchair) VALUES (%s,%s,%s,%s,%s,%s);"

        #cursor.execute("BEGIN;")
        cursor.executemany(sql, chunk)
        conn.commit()
    print("Successfully inserted table\n")

    cursor.close()


def trips_table(conn):
    cursor = conn.cursor()

    # init the table
    query = """CREATE TABLE Trips (
    	id SERIAL PRIMARY KEY NOT NULL,
    	trip_id INTEGER NOT NULL,
    	route_id INTEGER NOT NULL REFERENCES Routes(route_id),
    	service_id TEXT NOT NULL REFERENCES Calendar(service_id),
    	trip_headsign TEXT NOT NULL,
    	direction_id INTEGER NOT NULL,
    	shape_id INTEGER NOT NULL REFERENCES Forms(shape_id),
    	wheelchair_accessible INTEGER NOT NULL
    );"""
    cursor.execute(query)

    print("Inserting in table Trips and adding data")
    chunk_size = 500000
    with open("trips.txt", "r", encoding="utf-8") as file:
        file.readline()
        chunk = []
        i = 1
        for line in file:
            tokens = line.split(",")
            chunk.append((tokens[2],tokens[0],tokens[1],tokens[3],tokens[4],tokens[5],tokens[6]))
            sql = "INSERT INTO Trips (trip_id,route_id,service_id,trip_headsign,direction_id,shape_id,wheelchair_accessible) VALUES (%s,%s,%s,%s,%s,%s,%s);"
            if len(chunk) >= chunk_size:
                print(f"Created chunk #{i}. Executing query")
                cursor.executemany(sql, chunk)
                conn.commit()
                chunk = []
        if not chunk is None:
            print("Executing final query")
            cursor.executemany(sql, chunk)
            conn.commit()
    print("Successfully inserted table\n")
    cursor.close()


def stops_info_table(conn):
    cursor = conn.cursor()
    create = """CREATE TABLE IF NOT EXISTS StopsInfo(
    id SERIAL PRIMARY KEY,
    stop_name TEXT NOT NULL,
    route_id INTEGER NOT NULL,
    trip_headsign TEXT NOT NULL,
    days TEXT NOT NULL,
    arrival_time TEXT NOT NULL,
    stop_seq INTEGER NOT NULL
    );
    """
    print("Creating table StopsInfo")
    cursor.execute(create)

    sql = """INSERT INTO StopsInfo(stop_name,route_id,trip_headsign,days,arrival_time,stop_seq)
    SELECT stops.stop_name,trips.route_id,trips.trip_headsign,calendar.days,arrival_time,stoptimes.stop_seq
    FROM stoptimes JOIN trips ON stoptimes.trip_id = trips.trip_id
    JOIN calendar ON calendar.service_id = trips.service_id
    JOIN stops ON stoptimes.stop_id = stops.stop_id;
    """
    print("Inserting in table StopsInfo")
    cursor.execute(sql)

    print("Vacuuming database")
    cursor.execute("VACUUM FULL;")

    print("Creating index on StopsInfo")
    cursor.execute("CREATE INDEX StopsInfoIndex ON StopsInfo(route_id,stop_name);")

    cursor.close()


def map_table(conn):
    cursor = conn.cursor()
    query = """CREATE TABLE Map (
        id SERIAL PRIMARY KEY,
        trip_id INTEGER NOT NULL,
        trip_headsign TEXT NOT NULL, --i.e. direction
        route_id TEXT NOT NULL,
        stop_name TEXT NOT NULL,
        stop_id TEXT NOT NULL,
        stop_seq INTEGER NOT NULL,
        direction_id INTEGER NOT NULL,
        arrival_time INTEGER NOT NULL
    );
    """
    cursor.execute(query)
    print("\nInitialised table Map")
    print("Inserting table and adding data")
    #cursor.execute("BEGIN;")
    sql = """INSERT INTO Map(trip_id, trip_headsign, route_id, stop_name, stop_id, stop_seq, direction_id, arrival_time) SELECT Trips.trip_id, Trips.trip_headsign, Trips.route_id, Stops.stop_name, Stops.stop_id, StopsInfo.stop_seq, direction_id, 0 FROM (SELECT DISTINCT stop_name, route_id, trip_headsign, stop_seq FROM StopsInfo) AS StopsInfo JOIN Trips on Trips.trip_headsign = StopsInfo.trip_headsign AND Trips.route_id = StopsInfo.route_id JOIN Stops ON Stops.stop_name = StopsInfo.stop_name;"""
    cursor.execute(sql)
    conn.commit()
    print("Successfully inserted data in table Map\n")

    query = "CREATE INDEX MapIndex ON Map(trip_id,route_id,stop_id,direction_id);"
    print("Creating index for Map on trip_id, route_id, stop_id and direction_id")
    cursor.execute(query)
    print("Successfully created index for table Map\n")
    cursor.close()


def main():
    parser = argparse.ArgumentParser(description='Script to migrate from an sqlite3 database to a postgres database, to better handle concurrency tasks')
    parser.add_argument('-U', '--user', nargs=1, required=True, help='Username of the choosen database')
    parser.add_argument('-P', '--password', nargs=1, required=True, help='Password for the username')
    parser.add_argument('--no-download', action='store_true', help='Do not download the required files')

    args = parser.parse_args()

    if not args.no_download:
        download("https://www.stm.info/sites/default/files/gtfs/gtfs_stm.zip")

    init_database(args.user[0], args.password[0])


if __name__ == "__main__":
    main()
