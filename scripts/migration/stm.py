import aiohttp
import asyncio
import zipfile
import os
import asyncpg

from . import _helpers

__extracted_dir = "data/extracted/stm"
__zip_file = "data/downloads/stm.zip"

async def download_stm() -> bool:
    """
    Download and create respective directories
    @return True if succesful download and extracting or if no download needed
            False if failed download
    """
    url = "https://www.stm.info/sites/default/files/gtfs/gtfs_stm.zip"

    #Before doing anything, check if the data is already up to date or even if it exists
    folder_path = os.path.join(os.getcwd(), "data", "extracted", "stm")
    feed_info_path = os.path.join(folder_path, "feed_info.txt")
    if not os.path.exists(feed_info_path):
        print(f"File {feed_info_path} does not exist. Downloading stm data")
        if not await __download_stm(url):
            return False
        print("Sanitising files")
        for file in os.listdir(folder_path):
            print("Sanitising file: ", file)
            await __sanitise_file(os.path.join(folder_path, file))
        print("Sanitisation completed")
        return True

    # check today's date and compare with what is written in the feed_info file
    expiry_date_data: str
    with open(feed_info_path) as file:
        # could be broken if the feed publisher name also contains ',', but not very likely....
        metadata = file.readlines()[1].split(",") # the last line of the file contains the actual metadata,
        # TODO since the field is optinal, may need to use a try catch in case it doesn't exist...'
        expiry_date_data = metadata[4] # format: YYYYMMDD
    if _helpers._is_expiry_date_up_to_date(expiry_date_data):
        print("Downloaded data up to date, no downloading required")
        return True
    else:
        print(f"Data out of date since {expiry_date_data}. Downloading from {url}")
        if not await __download_stm(url):
            return False

    # sanitise every file by getting rid of empty lines
    print("Sanitising files")
    for file in os.listdir(folder_path):
        print("Sanitising file: ", file)
        await __sanitise_file(os.path.join(folder_path, file))
    print("Sanitisation completed")
    return True


async def __sanitise_file(file_path):
        proc = await asyncio.subprocess.create_subprocess_exec(
                "sed", "-i", "/^[[:space:]]*$/d", 
                file_path,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.PIPE,
            )
        _, stderr = await proc.communicate()
        if proc.returncode != 0:
            raise Exception(proc.returncode, 'sed', stderr)

async def __download_stm(url: str) -> bool:
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            if response.status == 200:
                await asyncio.to_thread(os.makedirs, __extracted_dir, exist_ok=True)
                
                with open(__zip_file, "wb") as file:
                    chunk_size = 4092
                    async for chunk in response.content.iter_chunked(chunk_size):
                        file.write(chunk)
                print(f"Downloaded {url} to {__zip_file} successfully")
                with zipfile.ZipFile(__zip_file, "r") as zip:
                    zip.extractall(f"{__extracted_dir}")
                print(f"Extracted file from {__zip_file}")
                return True
            else:
                print(f"Failed to download {url}")
                return False

async def init_database_stm(db_name: str, db_username: str, db_passwd: str, version: int, min_client_req_version: int, max_client_req_version: int) -> bool:
    """Initialise the data in the postgres database associated to that agency
    @return True if there was no error, False if there was
    """
    try:
        # TODO change the port to the one bound in Docker since we are using the host network for this script)
        dsn = f"postgres://{db_username}:{db_passwd}@0.0.0.0:5432/{db_name}"
        print("Initialising STM database")

        async with asyncpg.create_pool(
                dsn=dsn,
                command_timeout=60
            ) as pool:
            async with pool.acquire() as conn1, pool.acquire() as conn2, pool.acquire() as conn3:
                try:
                    end_date = await conn1.fetchval("""SELECT feed_end_date FROM "FeedInfo"; """)
                    if _helpers._is_expiry_date_up_to_date(end_date):
                        print("POSTGRES Database up to date")
                        return True
                    else:
                        print("Db out of date, remaking tables and data")
                except asyncpg.exceptions.UndefinedTableError:
                    print("No FeedInfo table, must initialise database")

                await conn1.execute("SET client_encoding TO 'UTF8'")
                await conn1.execute('DROP TABLE IF EXISTS "TMP_StopTimes"')
                await conn1.execute('DROP TABLE IF EXISTS "TMP_Stops"')
                await conn1.execute('DROP TABLE IF EXISTS "TMP_Trips"')
                await conn1.execute('DROP TABLE IF EXISTS "Calendar" CASCADE;')
                await conn1.execute('DROP TABLE IF EXISTS "CalendarDates" CASCADE;')
                await conn1.execute('DROP TABLE IF EXISTS "Forms" CASCADE;')
                await conn1.execute('DROP TABLE IF EXISTS "Routes" CASCADE;')
                await conn1.execute('DROP TABLE IF EXISTS "Shapes" CASCADE;')
                await conn1.execute('DROP TABLE IF EXISTS "StopsInfo" CASCADE;')
                await conn1.execute('DROP TABLE IF EXISTS "StopTimes" CASCADE;')
                await conn1.execute('DROP TABLE IF EXISTS "Stops" CASCADE;')
                await conn1.execute('DROP TABLE IF EXISTS "Trips" CASCADE;')
                await conn1.execute('DROP INDEX IF EXISTS "TripsIndex" CASCADE;')
                await conn1.execute('DROP TABLE IF EXISTS "Map" CASCADE;')
                await conn1.execute('DROP TABLE IF EXISTS "Config";')
                await conn1.execute('DROP TABLE IF EXISTS "FeedInfo";')

                await conn1.execute('DROP INDEX IF EXISTS "StopsInfoIndex";')
                await conn1.execute('DROP INDEX IF EXISTS "StopTimesIndex";')
                await conn1.execute('DROP INDEX IF EXISTS "MapIndex";')
                await __init_tmp_tables(conn1, conn2, conn3)

        conn = await asyncpg.connect(dsn=dsn)
        async with conn.transaction():
            await __calendar_table(conn)
            await __calendar_dates_table(conn)
            await __route_table(conn)
            # await __forms_table(conn)
            await __shapes_table(conn)
            await __trips_table(conn)

        async with conn.transaction():
            await __stops_table(conn)
            await __stop_times_table(conn)
            await __stops_info_table(conn)

        async with conn.transaction():
            await __map_table(conn)
            await _helpers._config_table(conn, version, min_client_req_version, max_client_req_version)
            await __feed_info(conn)

        await conn.execute('DROP TABLE IF EXISTS "TMP_StopTimes"')
        await conn.execute('DROP TABLE IF EXISTS "TMP_Stops"')
        await conn.execute('DROP TABLE IF EXISTS "TMP_Trips"')

        # ------------ Docker Modification: Check if the data is already downloaded to not repeat shit --------------
        # if os.path.exists(__zip_file):
        #     os.remove(__zip_file)
        #     print("Removed zip file")
        #     return True
        # --------------------------------------------------------------------------------------------------------

        # TODO remove these lines only when inside of docker because no user interactivity....
        # answer = input("Do you want to clean up the __extracted_dir from txt files? (y/n) ")
        # if answer == "yes" or answer == "y":
        #    print("Cleaning up")
        #    dir_content = os.listdir(__extracted_dir)
        #    for content in dir_content:
        #        if os.path.isfile(content) and content.endswith(".txt"):
        #            os.remove(f"{__extracted_dir}/*.txt")
        #    print("Cleaned up")
        # else:
        #    print("Not cleaning up")
        print("STM database initialisation done")
        return True

    except asyncpg.PostgresConnectionError:
        print(f"The username {db_username} does not exist. Aborting the script.")
        return False


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
    with open(f"{__extracted_dir}/calendar.txt", "r", encoding="utf-8") as file:
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
    with open(f"{__extracted_dir}/calendar_dates.txt", "rb") as file:
        await conn.copy_to_table(
            table_name='CalendarDates',
            source=file,
            format="csv",
            columns=["service_id", "date", "exception_type"],
            header=True
        )
    print("Successfully inserted table\n")


#TODO shape_id is not unique...
# async def __forms_table(conn: asyncpg.Connection):
#     await conn.execute("""CREATE TABLE "Forms"(
#     	id SERIAL PRIMARY KEY NOT NULL,
#     	shape_id INTEGER UNIQUE NOT NULL
#         );""")
#     print("Inserting in table Forms")
#     records = []
#     with open(f"{__extracted_dir}/shapes.txt", "r", encoding="utf-8") as file:
#         file.readline()
#         prev = ""
#         for line in file:
#             tokens = line.split(",")
#             shape_id = tokens[0]
#             if not shape_id == prev:
#                 records.append((int(tokens[0]),))
#                 prev = shape_id
#         sql = 'INSERT INTO "Forms" (shape_id) VALUES ($1);'
#         await conn.executemany(sql, records)
#     print("Successfully inserted table\n")


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
    with open(f"{__extracted_dir}/routes.txt", "r", encoding="utf-8") as file:
        file.readline()
        records = []
        for line in file:
            tokens = line.replace("\n", "").replace("'", "''").split(",")
            records.append((int(tokens[0]), tokens[3], int(tokens[4]), tokens[6]))
        sql = 'INSERT INTO "Routes" (route_id,route_long_name,route_type,route_color) VALUES ($1,$2,$3,$4);'
        await conn.executemany(sql, records)
    print("Successfully inserted table\n")


async def __shapes_table(conn: asyncpg.Connection):
    await conn.execute("""CREATE TABLE "Shapes"(
        id SERIAL PRIMARY KEY,
        shape_id INTEGER NOT NULL,
        shape_pt_lat REAL NOT NULL,
        shape_pt_long REAL NOT NULL,
        shape_pt_sequence INTEGER NOT NULL,
        route_pattern_id TEXT NOT NULL
        -- PRIMARY KEY(shape_id, shape_pt_sequence)
    );""")
    print("Initialised table shapes")

    print("Inserting in table Shapes")
    with open(f"{__extracted_dir}/shapes.txt", "rb") as file:
        await conn.copy_to_table(
            table_name="Shapes",
            source=file,
            format="csv",
            columns=["shape_id", "shape_pt_lat", "shape_pt_long", "shape_pt_sequence", "route_pattern_id"],
            header=True
        )
    print("Successfully inserted table\n")


async def __stop_times_table(conn: asyncpg.Connection):
    await conn.execute("""CREATE TABLE "StopTimes" (
        id SERIAL PRIMARY KEY,
        trip_id TEXT NOT NULL,
        arrival_time TEXT NOT NULL,
        departure_time TEXT NOT NULL,
        stop_id TEXT NOT NULL REFERENCES "Stops"(stop_id),
        stop_seq INTEGER NOT NULL
    ); """)
    print("Initialised table StopTimes")

    print("Inserting table and adding data")
    await conn.execute("""
        INSERT INTO "StopTimes" (trip_id, arrival_time, departure_time, stop_id, stop_seq)
        SELECT trip_id, arrival_time, departure_time, stop_id, stop_seq
        FROM "TMP_StopTimes";"""
    )

    query = 'CREATE INDEX "StopTimesIndex" ON "StopTimes"(stop_id,trip_id);'
    print("Creating index for StopTimes on stopid and tripid")
    await conn.execute(query)
    print("Successfully created index for table StopTimes\n")


async def __stops_table(conn: asyncpg.Connection):
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
    await conn.execute("""
        INSERT INTO "Stops" (stop_id,stop_code,stop_name,lat,long,wheelchair)
        SELECT stop_id,stop_code,stop_name,stop_lat,stop_lon,wheelchair_boarding
        FROM "TMP_Stops";"""
    )

    print("Successfully inserted table\n")


async def __trips_table(conn: asyncpg.Connection):
    await conn.execute("""CREATE TABLE "Trips" (
        id SERIAL PRIMARY KEY NOT NULL,
        trip_id TEXT NOT NULL,
        route_id INTEGER NOT NULL REFERENCES "Routes"(route_id),
        service_id TEXT NOT NULL REFERENCES "Calendar"(service_id),
        trip_headsign TEXT NOT NULL,
        direction_id INTEGER NOT NULL,
        shape_id INTEGER NOT NULL, --REFERENCES "Forms"(shape_id),
        wheelchair_accessible INTEGER NOT NULL
    );""")

    print("Inserting in table Trips and adding data")
    await conn.execute("""
        INSERT INTO "Trips" (trip_id,route_id,service_id,trip_headsign,direction_id,shape_id,wheelchair_accessible)
        SELECT trip_id,route_id,service_id,trip_headsign,direction_id,shape_id,wheelchair_accessible
        FROM "TMP_Trips";""")

    print("Successfully inserted table\n")


async def __stops_info_table(conn: asyncpg.Connection):
    await conn.execute("""CREATE TABLE IF NOT EXISTS "StopsInfo"(
        id SERIAL PRIMARY KEY,
        stop_name TEXT NOT NULL,
        route_id INTEGER NOT NULL, --REFERENCES "Routes"(route_id),
        trip_headsign TEXT NOT NULL,
        service_id TEXT NOT NULL REFERENCES "Calendar"(service_id),
        arrival_time TEXT NOT NULL,
        stop_seq INTEGER NOT NULL
        ); """)
    print("Created table StopsInfo")

    print("Inserting in table StopsInfo")

    await conn.execute("""INSERT INTO "StopsInfo"(stop_name,route_id,trip_headsign,service_id,arrival_time,stop_seq)
    SELECT "Stops".stop_name,"Trips".route_id,"Trips".trip_headsign,"Calendar".service_id,arrival_time,"StopTimes".stop_seq
    FROM "StopTimes" JOIN "Trips" ON "StopTimes".trip_id = "Trips".trip_id
    JOIN "Calendar" ON "Calendar".service_id = "Trips".service_id
    JOIN "Stops" ON "StopTimes".stop_id = "Stops".stop_id;
    """)

    await conn.execute('DROP INDEX IF EXISTS "StopTimesIndex";')
    await conn.execute('DROP TABLE IF EXISTS "StopTimes";')

    # print("Vacuuming database")
    # await conn.execute("VACUUM FULL;")

    print("Creating index on StopsInfo")
    await conn.execute('CREATE INDEX "StopsInfoIndex" ON "StopsInfo"(route_id,stop_name);')

async def __init_tmp_tables(conn1, conn2, conn3):
    print("Init tmp tables")
    await conn1.execute("""CREATE UNLOGGED TABLE "TMP_StopTimes" (
        id SERIAL PRIMARY KEY,
        trip_id TEXT NOT NULL,
        arrival_time TEXT NOT NULL,
        departure_time TEXT NOT NULL,
        stop_id TEXT NOT NULL,
        stop_seq INTEGER NOT NULL,
        pickup_type INTEGER NOT NULL
    ); """)
    await conn2.execute("""CREATE UNLOGGED TABLE "TMP_Stops" (
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
    #TODO route_pattern_id should be linkable with other tables
    await conn3.execute("""CREATE UNLOGGED TABLE "TMP_Trips" (
        route_id INTEGER NOT NULL,
        service_id TEXT NOT NULL,
        trip_id TEXT NOT NULL,
        trip_headsign TEXT NOT NULL,
        direction_id INTEGER NOT NULL,
        shape_id INTEGER NOT NULL,
        wheelchair_accessible INTEGER NOT NULL,
        route_pattern_id TEXT NOT NULL
    );""")

    print("Adding data to tmp tables")

    await asyncio.gather(
       __import_file(conn1, f"{__extracted_dir}/stop_times.txt", "TMP_StopTimes", ["trip_id", "arrival_time", "departure_time", "stop_id", "stop_seq", "pickup_type"]),
       __import_file(conn2, f"{__extracted_dir}/stops.txt",  "TMP_Stops", ["stop_id", "stop_code", "stop_name", "stop_lat", "stop_lon", "stop_url", "location_type", "parent_station", "wheelchair_boarding"]),
       __import_file(conn3, f"{__extracted_dir}/trips.txt", "TMP_Trips", ["route_id", "service_id", "trip_id", "trip_headsign", "direction_id", "shape_id", "wheelchair_accessible", "route_pattern_id"]),
    )
    print("Tmp file adding done")

async def __map_table(conn: asyncpg.Connection):
    await conn.execute("""CREATE TABLE "Map" (
        id SERIAL PRIMARY KEY,
        trip_id TEXT NOT NULL,
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

async def __feed_info(conn: asyncpg.Connection):
    print("Init table FeedInfo")
    await conn.execute("""CREATE TABLE "FeedInfo" (
        id SERIAL PRIMARY KEY,
        feed_publisher_name TEXT NOT NULL,
        feed_publisher_url TEXT NOT NULL,
        feed_lang TEXT NOT NULL,
        feed_start_date INTEGER NOT NULL,
        feed_end_date INTEGER NOT NULL,
        feed_version TEXT
    );""")
    await __import_file(conn, f"{__extracted_dir}/feed_info.txt", "FeedInfo", [ "feed_publisher_name", "feed_publisher_url" , "feed_lang", "feed_start_date", "feed_end_date", "feed_version"])
    print("Creating table FeedInfo")

async def __import_file(conn, filepath, table, columns):
    with open(filepath, "rb") as f:
        await conn.copy_to_table(
            table_name=table,
            source=f,
            format="csv",
            columns=columns,
            header=True
        )
