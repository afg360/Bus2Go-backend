#!/usr/bin/env python3

import asyncpg
import asyncio
import sqlite3
import gzip
import sys
import os

from migration import is_expiry_date_up_to_date


async def init_sample(overwrite: bool):
    await __generate_dbs(True, overwrite)

async def init_data(overwrite: bool) -> bool:
    return await __generate_dbs(False, overwrite)

async def __generate_dbs(is_sample: bool, overwrite: bool) -> bool:
    pg_conn: asyncpg.Connection = await asyncpg.connect(dsn=f'postgres://{os.environ.get("DB_USERNAME", "")}:{os.environ.get("DB_PASSWORD", "")}@0.0.0.0:8080/{os.environ.get("DB_1_NAME", "")}')
    lite_conn: sqlite3.Connection | None = None

    is_ok = True
    __file_stm = f"data/sqlite/stm_data.db" if is_sample else f"data/sqlite/stm_data_{os.environ.get("SQLITE_DB_1_VERSION", -1)}.db"
    try:
        if overwrite:
            if os.path.exists(__file_stm):
                os.remove(__file_stm)
            lite_conn = sqlite3.connect(__file_stm)
            await __copy(pg_conn, lite_conn, os.environ.get("DB_1_NAME", ""), is_sample, __file_stm) 
            lite_conn.close() 
            __compress(__file_stm)
            __generate_hash(__file_stm)
        else:
            lite_conn = sqlite3.connect(__file_stm)
            # in case we need to redo it from scratch
            if not os.path.exists(__file_stm):
                await __copy(pg_conn, lite_conn, os.environ.get("DB_1_NAME", ""), is_sample, __file_stm)
                if os.path.exists(f"{__file_stm}.gz"):
                    os.remove(f"{__file_stm}.gz")
                    __compress(__file_stm)
                if os.path.exists(f"{__file_stm}.gz.txt"):
                    os.remove(f"{__file_stm}.gz.txt")
                    __generate_hash(__file_stm)
            else: 
                lite_cursor = lite_conn.cursor()
                try:
                    lite_cursor.execute("SELECT feed_end_date FROM FeedInfo;")
                    # in case we only need to compress (usually in docker build fails)
                    if is_expiry_date_up_to_date(lite_cursor.fetchone()[0]):
                        print("SQLITE Db is up to date")
                        if not os.path.exists(f"{__file_stm}.gz"):
                            __compress(__file_stm)
                            __generate_hash(__file_stm)
                    else:
                        lite_conn.close()
                        os.remove(__file_stm)
                        lite_conn = sqlite3.connect(__file_stm)
                        await __copy(pg_conn, lite_conn, os.environ.get("DB_1_NAME", ""), is_sample, __file_stm)
                        if os.path.exists(f"{__file_stm}.gz"):
                            os.remove(f"{__file_stm}.gz")
                        __compress(__file_stm)
                        __generate_hash(__file_stm)
                except sqlite3.OperationalError:
                    print("SQLITE FeedInfo doesn't exist, need to init")
                    lite_conn = sqlite3.connect(__file_stm)
                    await __copy(pg_conn, lite_conn, os.environ.get("DB_1_NAME", ""), is_sample, __file_stm)
                    if os.path.exists(f"{__file_stm}.gz"):
                        os.remove(f"{__file_stm}.gz")
                    __compress(__file_stm)
                    __generate_hash(__file_stm)
                finally:
                    lite_cursor.close()
            lite_conn.close()

        await pg_conn.close()

        #TODO repopulate exo files

        # pg_conn = await asyncpg.connect(
        #     database = os.environ.get("DB_2_NAME", ""), 
        #     user = os.environ.get("DB_USERNAME", ""), 
        #     password = os.environ.get("DB_PASSWORD", "")
        # )
        # file_exo = "data/sqlite/exo_sample_data.db" if is_sample else f"data/sqlite/exo_data_{os.environ.get("SQLITE_DB_2_VERSION", "")}.db"

        # if overwrite:
        #     if os.path.exists(file_exo):
        #         os.remove(file_exo)
        #     lite_conn = sqlite3.connect(file_exo)
        #     await __copy(pg_conn, lite_conn, os.environ.get("DB_2_NAME", ""), is_sample, file_exo)
        #     __compress(file_exo)
        # else:
        #     if not os.path.exists(file_exo):
        #         lite_conn = sqlite3.connect(file_exo)
        #         await __copy(pg_conn, lite_conn, os.environ.get("DB_2_NAME", ""), is_sample, file_exo)
        #     else: print(f"Database file {file_exo} already exists.")
        #     if not os.path.exists(f"{file_exo}.gz"):
        #         __compress(file_exo)
        #     else: print(f"Compressed database file {file_exo} already exists.")
        #

    # except sqlite3.OperationalError as e: 
    #     print("Seems like you are missing the data/ directory. You must execute the script at the root level of the project.")
    #     print(e)
    #     is_ok = False

    except sqlite3.IntegrityError:
        print("The database already exists...")
        is_ok = False

    # except Exception as e:
    #     print("Some error occured")
    #     print(e)
    #     is_ok = False

    finally:
        if not pg_conn.is_closed():
            await pg_conn.close()
        if lite_conn is not None:
            lite_conn.close()
    return is_ok
async def __copy(pg_conn: asyncpg.Connection, lite_conn: sqlite3.Connection, db_name: str, is_sample: bool, file: str):
    """
    @param db_name The name of the POSTGRES database
    @param file The name of the sqlite3 database file 
    """
    print("Copying from postgres to sqlite")
    tables = await pg_conn.fetch("""SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public' AND table_type = 'BASE TABLE' AND table_name NOT LIKE 'Map' AND table_name NOT LIKE 'Config'
    ;""")

    for table in tables:
        lite_cursor = lite_conn.cursor()
        table_name: str = table['table_name']
        columns = await pg_conn.fetch("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = $1
            ORDER BY ordinal_position;
        """, table_name)

        pk_columns = await pg_conn.fetch("""
            SELECT kcu.column_name FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
            ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema
            WHERE tc.constraint_type = 'PRIMARY KEY' AND tc.table_name = $1
            AND tc.table_schema = 'public' ORDER BY kcu.ordinal_position;
        """, table_name)
        primary_keys = [col['column_name'] for col in pk_columns]

        fk_constraints = await pg_conn.fetch("""
            SELECT kcu.column_name, ccu.table_name AS foreign_table_name,
            ccu.column_name AS foreign_column_name FROM
            information_schema.table_constraints AS tc
            JOIN information_schema.key_column_usage AS kcu
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
            JOIN information_schema.constraint_column_usage AS ccu
            ON ccu.constraint_name = tc.constraint_name
            AND ccu.table_schema = tc.table_schema
            WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_name = $1;
        """, table_name)

        create_stmt = f'CREATE TABLE IF NOT EXISTS {table_name} (\n'
        col_defs = []

        for col in columns:
            pg_type = col['data_type']
            if pg_type in ('integer', 'bigint', 'smallint'):
                sqlite_type = 'INTEGER'
            elif pg_type in ('text', 'character varying', 'varchar', 'char'):
                sqlite_type = 'TEXT'
            elif pg_type in ('double precision', 'numeric', 'real'):
                sqlite_type = 'REAL'
            elif pg_type in ('boolean'):
                sqlite_type = 'INTEGER'
            else:
                sqlite_type = 'TEXT'

            nullable = "NULL" if col['is_nullable'] == 'YES' else "NOT NULL"

            if col['column_name'] in primary_keys and len(primary_keys) == 1:
                col_defs.append(f"{col['column_name']} {sqlite_type} PRIMARY KEY {nullable}")
            else:
                col_defs.append(f"{col['column_name']} {sqlite_type} {nullable}")

        if len(primary_keys) > 1:
            col_defs.append(f"PRIMARY KEY ({', '.join(primary_keys)})")

        for fk in fk_constraints:
            col_defs.append(f"FOREIGN KEY ({fk['column_name']}) REFERENCES {fk['foreign_table_name']}({fk['foreign_column_name']})")

        create_stmt += ",\n".join(col_defs)
        create_stmt += "\n)"
        print(f"Create statement: {create_stmt}")
        lite_cursor.execute(create_stmt)

        if is_sample:
            rows = await pg_conn.fetch(f'SELECT * FROM "{table_name}" LIMIT 70000;' )
            if rows is not None:
                col_names = [col['column_name'] for col in columns]
                placeholders = ", ".join(["?" for _ in col_names])

                insert_stmt = f"INSERT INTO {table_name} ({', '.join(col_names)}) VALUES ({placeholders})"

                for row in rows:
                    # Convert row to list, handling special types
                    values = []
                    for col, val in zip(columns, row):
                        if val is None:
                            values.append(None)
                        elif col['data_type'] == 'boolean':
                            values.append(1 if val else 0)
                        else:
                            values.append(str(val))

                    lite_cursor.execute(insert_stmt, values)
                lite_conn.commit()

        lite_cursor.close()
        if not is_sample:
            print(f"SQLITE: Inserting data in table {table_name} from database {db_name}")
            await __open_file(pg_conn, file, table_name)
            print(f"SQLITE: Data inserted in table {table_name} from database {db_name} succesfully\n")

    # print(f"Vaccuming {file} sqlite database")
    # lite_conn.execute("VACUUM;")
    # lite_conn.commit()
    print("Operation succeeded")


async def __open_file(pg_conn: asyncpg.Connection, file_name: str, table_name: str):
    with open(f"/tmp/{table_name}.csv", "wb") as csv_file:
        await pg_conn.copy_from_table(table_name=table_name, output=csv_file, format='csv', header=False)
    proc = await asyncio.create_subprocess_exec(
            "sqlite3", file_name, ".mode csv",
            f""".import /tmp/{table_name}.csv {table_name}""")
    await proc.wait()


def __compress(file_name: str):
    print(f"Compressing {file_name}")
    with open(file_name, 'rb') as f_in:
        with gzip.open(f"{file_name}.gz", 'wb') as f_out:
            chunk_size = 1024 * 1024
            while True:
                chunk = f_in.read(chunk_size)
                if not chunk:
                    break
                f_out.write(chunk)
    print(f"Compression succesful")

def __generate_hash(file_name: str) -> bool:
    import hashlib
    import traceback
    h = hashlib.sha256()
    print("Generating sha256 of", file_name + ".gz")
    try:
        with open(file_name + ".gz", "rb") as file:
            while True:
                chunk = file.read(h.block_size)
                if not chunk:
                    break
                h.update(chunk)
        with open(file_name + ".gz.txt", "w") as hash_file:
            hash_file.write(h.hexdigest())
            print(f"Done. SHA for {file_name}.gz.txt: ", h.hexdigest())
        return True
    except Exception as e:
        print("An exception occured: ")
        traceback.print_exception(e)
        return False

def __usage():
    print("Script that initialises sqlite3 databases by using data stored in the bus2go postgres databases.")
    print("Please be sure to first initialise postgres databases with the \"init_pg_db.py\" script.")
    print("Usage: build_sqlite3_dbs.py (-f/--full | -s/--sample) [-o/--overwrite]")
    sys.exit(1)

if __name__ == "__main__":
    if (len(sys.argv) > 3 or len(sys.argv) < 2): 
        __usage()
    overwrite = False
    if (len(sys.argv) == 3):
        if sys.argv[2] == "-o" or sys.argv[2] == "--overwrite":
            overwrite = True
        else: __usage()

    import dotenv
    dotenv.load_dotenv()
    if (sys.argv[1] == "-f" or sys.argv[1] == "--full"):
        asyncio.run(init_data(overwrite))
    elif (sys.argv[1] == "-s" or sys.argv[1] == "--sample"): 
        asyncio.run(init_sample(overwrite))
