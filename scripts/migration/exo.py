import asyncio
import aiohttp
import zipfile
import os
import asyncpg
import sys

__exo_directory = "data/downloads/exo"
__files = [
            "citcrc",   #autos Chambly-Richelieu-Carignan
            "cithsl",   #autos Haut-Saint-Laurent
            "citla",    #autos Laurentides
            "citpi",    #autos La Presqu'île
            "citlr",    #autos Le Richelain
            "citrous",  #autos Roussillon
            "citsv",    #autos Sorel-Varennes
            "citso",    #autos Sud-ouest
            "citvr",    #autos Vallée du Richelieu
            "mrclasso", #autos L'Assomption
            "mrclm",    #autos Terrebonne-Mascouche
            "trains",
            "omitsju",  #autos Sainte-Julie
            "lrrs"      #autos Le Richelain et Roussillon
    ]

async def download_exo():
    """Download and create respective directories"""
    jobs = [
        __download(
            "https://exo.quebec/xdata/" + file + "/google_transit.zip", 
            f"{__exo_directory}/{file}"
        ) for file in __files
    ]
    await asyncio.gather(*jobs)
    print(f"Downloaded succesfully")

async def __download(url: str, path: str):
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            if response.status == 200:
                os.makedirs(path, exist_ok=True)
                zip_file = f"{path}/data.zip"
                with open(zip_file, "wb") as file:
                    chunk_size = 4092
                    async for chunk in response.content.iter_chunked(chunk_size):
                        file.write(chunk)
                print(f"Downloaded {url} to {zip_file} successfully")
                with zipfile.ZipFile(zip_file, "r") as zip:
                    zip.extractall(f"{__exo_directory}")
                print(f"Extracted file from {zip_file}")
                os.remove(zip_file)
                print("Removed zip file")
            else:
                print(f"Failed to download {url}")

async def init_database_exo(db_name: str, db_user: str, db_passwd: str) -> None:
    """Initialise the data in the database associated to that agency"""
    try:
        #TODO for exo...
        conn = await asyncpg.connect(
            database = db_name,
            user = db_user,
            password = db_passwd
        )
        
        await conn.execute("SET client_encoding TO 'UTF8'")
        await conn.execute('DROP TABLE IF EXISTS "Calendar" CASCADE;')
        await conn.execute('DROP TABLE IF EXISTS "CalendarDates" CASCADE;')
        await conn.execute('DROP TABLE IF EXISTS "Forms" CASCADE;')
        await conn.execute('DROP TABLE IF EXISTS "Routes" CASCADE;')
        await conn.execute('DROP TABLE IF EXISTS "Shapes" CASCADE;')
        await conn.execute('DROP TABLE IF EXISTS "StopsInfo" CASCADE;')
        await conn.execute('DROP TABLE IF EXISTS "StopTimes" CASCADE;')
        await conn.execute('DROP TABLE IF EXISTS "Stops" CASCADE;')
        await conn.execute('DROP TABLE IF EXISTS "Trips" CASCADE;')
        await conn.execute('DROP INDEX IF EXISTS "TripsIndex" CASCADE;')
        await conn.execute('DROP TABLE IF EXISTS "Map" CASCADE;')
        await conn.execute('DROP INDEX IF EXISTS "StopsInfoIndex";')
        await conn.execute('DROP INDEX IF EXISTS "StopTimesIndex";')
        await conn.execute('DROP INDEX IF EXISTS "MapIndex";')

        await __calendar_table(conn)
        await __calendar_dates_table(conn)
        await __route_table(conn)
        await __forms_table(conn)
        await __shapes_table(conn)
        await __trips_table(conn)
        await __stops_table(conn)
        await __stop_times_table(conn)
        await __stops_info_table(conn)
        await __map_table(conn)

        await conn.close()

        answer = input("Do you want to clean up the __exo_directory from txt files? (y/n) ")
        if answer == "yes" or answer == "y":
            print("Cleaning up")
            dir_content = os.listdir(__exo_directory)
            for content in dir_content:
                if os.path.isfile(content) and content.endswith(".txt"):
                    os.remove(f"{__exo_directory}/*.txt")
            print("Cleaned up")
        else:
            print("Not cleaning up")

    except asyncpg.PostgresConnectionError:
        print(f"The username {settings.DB_USERNAME} does not exist. Aborting the script.")
        sys.exit(1)


async def __calendar_table(conn: asyncpg.Connection):
    await conn.execute("""CREATE TABLE "Calendar" (
    	service_id TEXT PRIMARY KEY NOT NULL,
        days VARCHAR(7) NOT NULL,
    	start_date INTEGER NOT NULL,
    	end_date INTEGER NOT NULL
        );"""
    )
    print("Initialised table Calendar")

    print("Inserting in table Calendar and adding data")
    with open(f"{__exo_directory}/calendar.txt", "r", encoding="utf-8") as file:
        file.readline()
        async with conn.transaction():

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
                sql = f'INSERT INTO "Calendar" (service_id,days,start_date,end_date) VALUES ($1,$2,$3,$4);'
                await conn.execute(sql, tokens[0], days, int(tokens[8]), int(tokens[9]))
    print("Successfully inserted table\n")

async def __calendar_dates_table(conn: asyncpg.Connection):
    await conn.execute("""CREATE TABLE "CalendarDates"(
    	service_id TEXT REFERENCES "Calendar"(service_id) NOT NULL,
        date TEXT NOT NULL,
        exception_type INTEGER NOT NULL,
        PRIMARY KEY (service_id, date)
        );""")
    print("Initialised table CalendarDates")

    print("Inserting in table CalendarDates")
    #asyncpg expects a binary stream, so explicitely state that the data is in csv format
    with open(f"{__exo_directory}/calendar_dates.txt", "rb") as file:
        await conn.copy_to_table(
            table_name='CalendarDates',
            source=file,
            format="csv",
            columns=["service_id", "date", "exception_type"],
            header=True
        )
    print("Successfully inserted table\n")


async def __forms_table(conn: asyncpg.Connection):
    await conn.execute("""CREATE TABLE "Forms"(
    	id SERIAL PRIMARY KEY NOT NULL,
    	shape_id INTEGER UNIQUE NOT NULL
        );""")
    print("Inserting in table Forms")
    records = []
    with open(f"{__exo_directory}/shapes.txt", "r", encoding="utf-8") as file:
        file.readline()
        
        async with conn.transaction():
            prev = ""
            for line in file:
                tokens = line.split(",")
                shape_id = tokens[0]
                if not shape_id == prev:
                    records.append((int(tokens[0]),))
                    prev = shape_id
            sql = 'INSERT INTO "Forms" (shape_id) VALUES ($1);'
            await conn.executemany(sql, records)
    print("Successfully inserted table\n")


async def __route_table(conn: asyncpg.Connection):
    await conn.execute("""CREATE TABLE "Routes" (
        id SERIAL PRIMARY KEY NOT NULL,
        route_id INTEGER UNIQUE NOT NULL,
        route_long_name TEXT NOT NULL,
        route_type INTEGER NOT NULL,
        route_color TEXT NOT NULL
        );""")
    print("Initialised table routes")

    print("Inserting in table Route and adding data")
    with open(f"{__exo_directory}/routes.txt", "r", encoding="utf-8") as file:
        file.readline()
        records = []
        async with conn.transaction():
            for line in file:
                tokens = line.replace("\n", "").replace("'", "''").split(",")
                records.append((int(tokens[0]), tokens[3], int(tokens[4]), tokens[6]))
            sql = 'INSERT INTO "Routes" (route_id,route_long_name,route_type,route_color) VALUES ($1,$2,$3,$4);'
            await conn.executemany(sql, records)
    print("Successfully inserted table\n")


async def __shapes_table(conn: asyncpg.Connection):
    await conn.execute("""CREATE TABLE "Shapes"(
        id SERIAL PRIMARY KEY,
        shape_id INTEGER NOT NULL REFERENCES "Forms"(shape_id),
        lat REAL NOT NULL,
        long REAL NOT NULL,
        sequence INTEGER NOT NULL
    );""")
    print("Initialised table shapes")

    print("Inserting in table Shapes")
    with open(f"{__exo_directory}/shapes.txt", "rb") as file:
        await conn.copy_to_table(
            table_name="Shapes",
            source=file,
            format="csv",
            columns=["shape_id", "lat", "long", "sequence"],
            header=True
        )
    print("Successfully inserted table\n")


async def __stop_times_table(conn: asyncpg.Connection):
    await conn.execute("""CREATE TABLE "StopTimes" (
        id SERIAL PRIMARY KEY,
        trip_id INTEGER NOT NULL,
        arrival_time TEXT NOT NULL,
        departure_time TEXT NOT NULL,
        stop_id TEXT NOT NULL REFERENCES "Stops"(stop_id),
        stop_seq INTEGER NOT NULL
    ); """)
    print("Initialised tmp table StopTimes")

    print("Inserting table and adding data")
    with open(f"{__exo_directory}/stop_times.txt", "rb") as file:
        await conn.copy_to_table(
            table_name="StopTimes",
            source=file,
            format="csv",
            columns=["trip_id", "arrival_time", "departure_time", "stop_id", "stop_seq"],
            header=True
        )
    query = 'CREATE INDEX "StopTimesIndex" ON "StopTimes"(stop_id,trip_id);'
    print("Creating index for StopTimes on stopid and tripid")
    await conn.execute(query)
    print("Successfully created index for table StopTimes\n")


async def __stops_table(conn: asyncpg.Connection):
    await conn.execute("""CREATE TEMP TABLE "TMP_Stops"(
        stop_id TEXT UNIQUE NOT NULL,
        stop_code INTEGER NOT NULL,
        stop_name TEXT NOT NULL,
        stop_lat REAL NOT NULL,
        stop_lon REAL NOT NULL,
        stop_url TEXT,
        location_type TEXT,
        parent_station TEXT,
        wheelchair_boarding INTEGER NOT NULL
    )
    """)

    await conn.execute("""CREATE TABLE "Stops" (
        id SERIAL PRIMARY KEY NOT NULL,
        stop_id TEXT UNIQUE NOT NULL,
        stop_code INTEGER NOT NULL,
        stop_name TEXT NOT NULL,
        lat REAL NOT NULL,
        long REAL NOT NULL,
        wheelchair INTEGER NOT NULL
    );""")
    print("Initialised table stops")

    print("Inserting in table Stops")
    with open(f"{__exo_directory}/stops.txt", "rb") as file:
        await conn.copy_to_table(
            table_name='TMP_Stops',
            source=file,
            format="csv",
            columns=["stop_id", "stop_code", "stop_name", "stop_lat", "stop_lon", "stop_url", "location_type", "parent_station", "wheelchair_boarding"],
            header=True
        )
        print("Successfully inserted into tmp table")

    async with conn.transaction():
        await conn.execute("""
            INSERT INTO "Stops" (stop_id,stop_code,stop_name,lat,long,wheelchair)
            SELECT stop_id,stop_code,stop_name,stop_lat,stop_lon,wheelchair_boarding
            FROM "TMP_Stops";"""
        )

    print("Successfully inserted table\n")


async def __trips_table(conn: asyncpg.Connection):
    await conn.execute("""CREATE TEMP TABLE "TMP_Trips"(
        route_id INTEGER NOT NULL,
        service_id TEXT NOT NULL,
        trip_id INTEGER NOT NULL,
        trip_headsign TEXT NOT NULL,
        direction_id INTEGER NOT NULL,
        shape_id INTEGER NOT NULL,
        wheelchair_accessible INTEGER NOT NULL,
        note_fr TEXT,
        note_en TEXT
    );""")
    await conn.execute("""CREATE TABLE "Trips" (
        id SERIAL PRIMARY KEY NOT NULL,
        trip_id INTEGER NOT NULL,
        route_id INTEGER NOT NULL REFERENCES "Routes"(route_id),
        service_id TEXT NOT NULL REFERENCES "Calendar"(service_id),
        trip_headsign TEXT NOT NULL,
        direction_id INTEGER NOT NULL,
        shape_id INTEGER NOT NULL REFERENCES "Forms"(shape_id),
        wheelchair_accessible INTEGER NOT NULL
    );""")

    print("Inserting in table Trips and adding data")
    with open(f"{__exo_directory}/trips.txt", "rb") as file:
        await conn.copy_to_table(
            table_name='TMP_Trips',
            source=file,
            format="csv",
            columns=["route_id", "service_id", "trip_id", "trip_headsign", "direction_id", "shape_id", "wheelchair_accessible", "note_fr", "note_en"],
            header=True
        )
        print("Successfully inserted into tmp table\n")

    async with conn.transaction():
        await conn.execute("""
            INSERT INTO "Trips" (trip_id,route_id,service_id,trip_headsign,direction_id,shape_id,wheelchair_accessible)
            SELECT trip_id,route_id,service_id,trip_headsign,direction_id,shape_id,wheelchair_accessible
            FROM "TMP_Trips";""")

    print("Successfully inserted table\n")


async def __stops_info_table(conn: asyncpg.Connection):
    await conn.execute("""CREATE TABLE IF NOT EXISTS "StopsInfo"(
        id SERIAL PRIMARY KEY,
        stop_name TEXT NOT NULL,
        route_id INTEGER NOT NULL,
        trip_headsign TEXT NOT NULL,
        service_id TEXT NOT NULL REFERENCES "Calendar"(service_id),
        arrival_time TEXT NOT NULL,
        stop_seq INTEGER NOT NULL
        ); """)
    print("Created table StopsInfo")

    print("Inserting in table StopsInfo")
    async with conn.transaction():
        await conn.execute("""INSERT INTO "StopsInfo"(stop_name,route_id,trip_headsign,service_id,arrival_time,stop_seq)
        SELECT "Stops".stop_name,"Trips".route_id,"Trips".trip_headsign,"Calendar".service_id,arrival_time,"StopTimes".stop_seq
        FROM "StopTimes" JOIN "Trips" ON "StopTimes".trip_id = "Trips".trip_id
        JOIN "Calendar" ON "Calendar".service_id = "Trips".service_id
        JOIN "Stops" ON "StopTimes".stop_id = "Stops".stop_id;
        """)

    print("Vacuuming database")
    await conn.execute("VACUUM FULL;")

    print("Creating index on StopsInfo")
    await conn.execute('CREATE INDEX "StopsInfoIndex" ON "StopsInfo"(route_id,stop_name);')


async def __map_table(conn: asyncpg.Connection):
    await conn.execute("""CREATE TABLE "Map" (
        id SERIAL PRIMARY KEY,
        trip_id INTEGER NOT NULL,
        trip_headsign TEXT NOT NULL, --i.e. direction
        route_id TEXT NOT NULL,
        stop_name TEXT NOT NULL,
        stop_id TEXT NOT NULL,
        stop_seq INTEGER NOT NULL,
        direction_id INTEGER NOT NULL,
        arrival_time INTEGER NOT NULL
    );""")
    print("\nInitialised table Map")

    print("Inserting table and adding data")
    async with conn.transaction():
        await conn.execute("""INSERT INTO "Map"(trip_id, trip_headsign, route_id, stop_name, stop_id, stop_seq, direction_id, arrival_time) 
        SELECT "Trips".trip_id, "Trips".trip_headsign, "Trips".route_id, "Stops".stop_name, "Stops".stop_id, "StopsInfo".stop_seq, direction_id, 0 
        FROM (SELECT DISTINCT stop_name, route_id, trip_headsign, stop_seq FROM "StopsInfo") AS "StopsInfo" 
        JOIN "Trips" on "Trips".trip_headsign = "StopsInfo".trip_headsign AND "Trips".route_id = "StopsInfo".route_id 
        JOIN "Stops" ON "Stops".stop_name = "StopsInfo".stop_name;""")
    print("Successfully inserted data in table Map\n")

    print("Creating index for Map on trip_id, route_id, stop_id and direction_id")
    await conn.execute('CREATE INDEX "MapIndex" ON "Map"(trip_id,route_id,stop_id,direction_id);'
)
    print("Successfully created index for table Map\n")

