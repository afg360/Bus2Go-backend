import asyncio
import aiohttp
import zipfile
import os
import asyncpg
import sys

__exo_directory = "data/downloads/exo"
__agencies = [
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
            "https://exo.quebec/xdata/" + agency + "/google_transit.zip", 
            f"{__exo_directory}/{agency}"
        ) for agency in __agencies
    ]
    await asyncio.gather(*jobs)
    print(f"Downloaded succesfully")

async def __download(url: str, path: str):
    print(f"{path}/data.zip")
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            if response.status == 200:
                await asyncio.to_thread(os.makedirs, path, exist_ok=True)
                zip_file = f"{path}/data.zip"
                with open(zip_file, "wb") as file:
                    chunk_size = 4092
                    async for chunk in response.content.iter_chunked(chunk_size):
                        file.write(chunk)
                print(f"Downloaded {url} to {zip_file} successfully")
                with zipfile.ZipFile(zip_file, "r") as zip:
                    zip.extractall(path)
                print(f"Extracted file from {zip_file}")
                os.remove(zip_file)
                print("Removed zip file")
            else:
                print(f"Failed to download {url}")


#TODO delete tmp tables as cleanup mechanism if script fails...?
async def init_database_exo(db_name: str, db_username: str, db_passwd: str) -> None:
    """Initialise the data in the database associated to that agency"""
    try:
        async with asyncpg.create_pool(
            database = db_name,
            user = db_username,
            password = db_passwd,
            min_size = 1,
            max_size = len(__agencies) + 1
        ) as pool:
            async with pool.acquire() as conn:
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
                #await conn.execute('DROP TABLE IF EXISTS "Map" CASCADE;')
                await conn.execute('DROP INDEX IF EXISTS "StopsInfoIndex";')
                await conn.execute('DROP INDEX IF EXISTS "StopTimesIndex";')
                #await conn.execute('DROP INDEX IF EXISTS "MapIndex";')

            async def __create_tables(agency):
                async with pool.acquire() as conn:
                    await __calendar_table(conn, agency)
                    await __calendar_dates_table(conn, agency)
                    await __route_table(conn, agency)
                    await __forms_table(conn, agency)
                    await __shapes_table(conn, agency)
                    await __trips_table(conn, agency)
                    await __stops_table(conn, agency)
                    await __stop_times_table(conn, agency)
                    #await __map_table(conn)

            for agency in __agencies:
                await __create_tables(agency) 
            #await asyncio.gather(*[__create_tables(agency) for agency in __agencies])

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
        print(f"The username {db_username} does not exist. Aborting the script.")
        sys.exit(1)


async def __calendar_table(conn: asyncpg.Connection, agency: str):
    async with conn.transaction():
        await conn.execute("""CREATE TABLE IF NOT EXISTS "Calendar" (
            service_id TEXT PRIMARY KEY NOT NULL,
            days VARCHAR(7) NOT NULL,
            start_date INTEGER NOT NULL,
            end_date INTEGER NOT NULL
            );"""
        )
    print("Initialised table Calendar")

    print("Inserting in table Calendar and adding data")
    with open(f"{__exo_directory}/{agency}/calendar.txt", "r", encoding="utf-8") as file:
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
            sql = f'INSERT INTO "Calendar" (service_id,days,start_date,end_date) VALUES ($1,$2,$3,$4);'
            async with conn.transaction():
                await conn.execute(sql, tokens[0], days, int(tokens[8]), int(tokens[9]))
    print("Successfully inserted table\n")

async def __calendar_dates_table(conn: asyncpg.Connection, agency: str):
    async with conn.transaction():
        await conn.execute("""CREATE TABLE IF NOT EXISTS "CalendarDates"(
            service_id TEXT REFERENCES "Calendar"(service_id) NOT NULL,
            date TEXT NOT NULL,
            exception_type INTEGER NOT NULL,
            PRIMARY KEY (service_id, date)
            );""")
    print("Initialised table CalendarDates")

    print("Inserting in table CalendarDates")
    #asyncpg expects a binary stream, so explicitely state that the data is in csv format
    with open(f"{__exo_directory}/{agency}/calendar_dates.txt", "rb") as file:
        await conn.copy_to_table(
            table_name='CalendarDates',
            source=file,
            format="csv",
            columns=["service_id", "date", "exception_type"],
            header=True
        )
    print("Successfully inserted table\n")


async def __forms_table(conn: asyncpg.Connection, agency: str):
    #TODO can be optimised to use queries instead of reading from files...
    async with conn.transaction():
        await conn.execute("""CREATE TABLE IF NOT EXISTS "Forms"(
            id SERIAL PRIMARY KEY NOT NULL,
            shape_id TEXT UNIQUE NOT NULL
            );""")
    print("Inserting in table Forms")
    records = []
    with open(f"{__exo_directory}/{agency}/shapes.txt", "r", encoding="utf-8") as file:
        file.readline()
        async with conn.transaction():
            prev = ""
            for line in file:
                tokens = line.split(",")
                shape_id = tokens[0]
                #shape_id = f"{agency}-{tokens[0]}"
                #FIXME use list.contains instead...?
                if not shape_id == prev:
                    records.append((shape_id,))
                    #records.append((shape_id,))
                    prev = shape_id
            sql = 'INSERT INTO "Forms" (shape_id) VALUES ($1);'
            await conn.executemany(sql, records)
    print("Successfully inserted table\n")


async def __route_table(conn: asyncpg.Connection, agency: str):
    async with conn.transaction():
        await conn.execute("""CREATE TABLE IF NOT EXISTS "Routes" (
            id SERIAL PRIMARY KEY NOT NULL,
            route_id TEXT UNIQUE NOT NULL,
            route_long_name TEXT NOT NULL,
            route_type INTEGER NOT NULL,
            route_color TEXT NOT NULL,
            route_text_color TEXT NOT NULL
            );""")
    print("Initialised table routes")

    print("Inserting in table Route and adding data")
    with open(f"{__exo_directory}/{agency}/routes.txt", "r", encoding="utf-8") as file:
        file.readline()
        records = []
        async with conn.transaction():
            for line in file:
                tokens = line.replace("\n", "").replace("'", "''").split(",")
                records.append((f"{agency}-{tokens[0]}", tokens[3], int(tokens[4]), tokens[5], tokens[6]))
            sql = 'INSERT INTO "Routes" (route_id,route_long_name,route_type,route_color, route_text_color) VALUES ($1,$2,$3,$4, $5);'
            await conn.executemany(sql, records)
    print("Successfully inserted table\n")


async def __shapes_table(conn: asyncpg.Connection, agency: str):
    async with conn.transaction():
        await conn.execute("""CREATE TEMP TABLE IF NOT EXISTS "TMP_Shapes"(
            shape_id TEXT,
            shape_pt_lat REAL NOT NULL,
            shape_pt_long REAL NOT NULL,
            shape_pt_sequence INTEGER NOT NULL,
            shape_dist_traveled REAL NOT NULL
        );""")
        await conn.execute("""CREATE TABLE IF NOT EXISTS "Shapes"(
            id SERIAL PRIMARY KEY,
            shape_id TEXT NOT NULL REFERENCES "Forms"(shape_id),
            lat REAL NOT NULL,
            long REAL NOT NULL,
            sequence INTEGER NOT NULL
        );""")
    print("Initialised table shapes")

    print("Inserting in table Shapes")
    with open(f"{__exo_directory}/{agency}/shapes.txt", "rb") as file:
        async with conn.transaction():
            await conn.copy_to_table(
                table_name="TMP_Shapes",
                source=file,
                format="csv",
                columns=["shape_id", "shape_pt_lat", "shape_pt_long", "shape_pt_sequence", "shape_dist_traveled"],
                header=True
            )

    async with conn.transaction():
        await conn.execute(f"""INSERT INTO "Shapes" (shape_id,lat,long,sequence) SELECT shape_id, shape_pt_lat, shape_pt_long, shape_pt_sequence FROM "TMP_Shapes";""")
    print("Successfully inserted table\n")


async def __stop_times_table(conn: asyncpg.Connection, agency: str):
    async with conn.transaction():
        await conn.execute("""CREATE TEMP TABLE "TMP_StopTimes" (
            trip_id TEXT NOT NULL,
            arrival_time TEXT NOT NULL,
            departure_time TEXT NOT NULL,
            stop_id TEXT NOT NULL,
            stop_sequence INTEGER NOT NULL,
            pickup_type INTEGER NOT NULL,
            drop_off_type INTEGER NOT NULL,
            shape_dist_traveled REAL NOT NULL,
            timepoint INTEGER NOT NULL,
            platform_track TEXT --nullable
        ); """)
        await conn.execute("""CREATE TABLE IF NOT EXISTS "StopTimes" (
            id SERIAL PRIMARY KEY,
            trip_id TEXT NOT NULL REFERENCES "Trips"(trip_id),
            arrival_time TEXT NOT NULL,
            departure_time TEXT NOT NULL,
            stop_id TEXT NOT NULL REFERENCES "Stops"(stop_id),
            stop_seq INTEGER NOT NULL
        ); """)
    print("Initialised tmp table StopTimes")

    print("Inserting table and adding data")
    with open(f"{__exo_directory}/{agency}/stop_times.txt", "rb") as file:
        async with conn.transaction():
            await conn.copy_to_table(
                table_name="TMP_StopTimes",
                source=file,
                format="csv",
                columns=["trip_id", "arrival_time", "departure_time", "stop_id", "stop_sequence", "pickup_type", "drop_off_type", "shape_dist_traveled", "timepoint", "platform_track"],
                header=True
            )

    async with conn.transaction():
        await conn.execute(f"""
            INSERT INTO "StopTimes"(trip_id, arrival_time, departure_time, stop_id, stop_seq) SELECT trip_id, arrival_time, departure_time, '{agency}-'||stop_id, stop_sequence FROM "TMP_StopTimes";
        """)
        await conn.execute("""DROP TABLE "TMP_StopTimes";""")

    #query = 'CREATE INDEX "StopTimesIndex" ON "StopTimes"(stop_id,trip_id);'
    #print("Creating index for StopTimes on stopid and tripid")
    #await conn.execute(query)
    #print("Successfully created index for table StopTimes\n")


async def __stops_table(conn: asyncpg.Connection, agency: str):
    async with conn.transaction():
        if agency == "trains":
            #FIXME some stop_id are written as __stop_id, perhaps skip those...
            await conn.execute("""CREATE TEMP TABLE "TMP_Stops"(
                stop_id TEXT UNIQUE NOT NULL,
                stop_name TEXT NOT NULL,
                stop_desc TEXT,
                stop_lat REAL NOT NULL,
                stop_lon REAL NOT NULL,
                zone_id INTEGER, --NOT NULL,
                stop_code TEXT NOT NULL,
                wheelchair_boarding INTEGER NOT NULL
            );""")
        else:
            await conn.execute("""CREATE TEMP TABLE "TMP_Stops"(
                stop_id TEXT UNIQUE NOT NULL,
                stop_name TEXT NOT NULL,
                stop_lat REAL NOT NULL,
                stop_lon REAL NOT NULL,
                zone_id INTEGER NOT NULL,
                stop_code TEXT NOT NULL,
                wheelchair_boarding INTEGER NOT NULL
            );""")

        await conn.execute("""CREATE TABLE IF NOT EXISTS "Stops" (
            id SERIAL PRIMARY KEY NOT NULL,
            stop_id TEXT UNIQUE NOT NULL,
            stop_name TEXT NOT NULL,
            lat REAL NOT NULL,
            long REAL NOT NULL,
            stop_code TEXT NOT NULL,
            wheelchair INTEGER NOT NULL
        );""")
    print("Initialised table stops")

    print("Inserting in table Stops")
    with open(f"{__exo_directory}/{agency}/stops.txt", "rb") as file:
        async with conn.transaction():
            if agency == "trains":
                await conn.copy_to_table(
                    table_name='TMP_Stops',
                    source=file,
                    format="csv",
                    columns=["stop_id", "stop_name", "stop_desc", "stop_lat", "stop_lon", "zone_id", "stop_code", "wheelchair_boarding"],
                    header=True
                )
            else:
                await conn.copy_to_table(
                    table_name='TMP_Stops',
                    source=file,
                    format="csv",
                    columns=["stop_id", "stop_name", "stop_lat", "stop_lon", "zone_id", "stop_code", "wheelchair_boarding"],
                    header=True
                )
        print("Successfully inserted into tmp table")

    async with conn.transaction():
        await conn.execute(f"""
            INSERT INTO "Stops" (stop_id,stop_name,lat,long,stop_code,wheelchair)
            SELECT '{agency}-'||stop_id,stop_name,stop_lat,stop_lon,stop_code,wheelchair_boarding
            FROM "TMP_Stops";
        """)
        await conn.execute("""DROP TABLE "TMP_Stops";""")

    print("Successfully inserted table\n")


async def __trips_table(conn: asyncpg.Connection, agency: str):
    #TODO perhaps add a agency- prefix to trip_ids
    async with conn.transaction():
        await conn.execute("""CREATE TEMP TABLE "TMP_Trips"(
            route_id TEXT NOT NULL,
            service_id TEXT NOT NULL,
            trip_id TEXT NOT NULL,
            trip_headsign TEXT NOT NULL,
            direction_id INTEGER NOT NULL,
            shape_id TEXT NOT NULL,
            trip_short_name TEXT NOT NULL,
            wheelchair_accessible INTEGER NOT NULL,
            bikes_allowed INTEGER NOT NULL
        );""")
        await conn.execute("""CREATE TABLE IF NOT EXISTS "Trips" (
            id SERIAL PRIMARY KEY NOT NULL,
            trip_id TEXT UNIQUE NOT NULL,
            route_id TEXT NOT NULL REFERENCES "Routes"(route_id),
            service_id TEXT NOT NULL REFERENCES "Calendar"(service_id),
            trip_headsign TEXT NOT NULL,
            direction_id INTEGER NOT NULL,
            shape_id TEXT NOT NULL REFERENCES "Forms"(shape_id),
            wheelchair INTEGER NOT NULL
        );""")

    print("Inserting in table Trips and adding data")
    with open(f"{__exo_directory}/{agency}/trips.txt", "rb") as file:
        async with conn.transaction():
            await conn.copy_to_table(
                table_name='TMP_Trips',
                source=file,
                format="csv",
                columns=["route_id", "service_id", "trip_id", "trip_headsign", "direction_id", "shape_id", "trip_short_name", "wheelchair_accessible", "bikes_allowed"],
                header=True
            )
        print("Successfully inserted into tmp table\n")

    async with conn.transaction():
        await conn.execute(f"""
            INSERT INTO "Trips" (trip_id,route_id,service_id,trip_headsign,direction_id,shape_id,wheelchair)
            SELECT trip_id,'{agency}-'||route_id,service_id,trip_headsign,direction_id,shape_id,wheelchair_accessible
            FROM "TMP_Trips";""")
        await conn.execute("""DROP TABLE "TMP_Trips";""")

    print("Successfully inserted table\n")


async def __map_table(conn: asyncpg.Connection):
    pass
    #TODO
    async with conn.transaction():
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
    async with conn.transaction():
        await conn.execute('CREATE INDEX "MapIndex" ON "Map"(trip_id,route_id,stop_id,direction_id);'
    )
    print("Successfully created index for table Map\n")

