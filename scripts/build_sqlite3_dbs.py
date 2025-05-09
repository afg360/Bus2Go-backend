#!/usr/bin/env python3

import asyncpg
import asyncio
import sqlite3
import gzip

import sys
import os
from pathlib import Path

src_path = Path(__file__).parents[1]
sys.path.append(str(src_path))

from src import settings

async def execute(is_sample: bool, overwrite: bool):
    pg_conn: asyncpg.Connection = await asyncpg.connect(
        database = settings.DB_1_NAME, 
        user = settings.DB_USERNAME, 
        password = settings.DB_PASSWORD
    )
    file_stm = "data/stm_sample_data.db" if is_sample else "data/stm_data.db"
    lite_conn: sqlite3.Connection | None = None

    try:
        if overwrite:
            if os.path.exists(file_stm):
                os.remove(file_stm)
            lite_conn = sqlite3.connect(file_stm)
            await __copy(pg_conn, lite_conn, settings.DB_1_NAME, is_sample, file_stm)
            lite_conn.close()
            __compress(file_stm)
        else:
            if not os.path.exists(file_stm):
                lite_conn = sqlite3.connect(file_stm)
                await __copy(pg_conn, lite_conn, settings.DB_1_NAME, is_sample, file_stm)
                lite_conn.close()
            else: print(f"Database file {file_stm} already exists.")
            if not os.path.exists(f"{file_stm}.gz"):
                __compress(file_stm)
            else: print(f"Compressed database file {file_stm} already exists.")
        await pg_conn.close()

        pg_conn = await asyncpg.connect(
            database = settings.DB_2_NAME, 
            user = settings.DB_USERNAME, 
            password = settings.DB_PASSWORD
        )
        file_exo = "data/exo_sample_data.db" if is_sample else "data/exo_data.db"

        if overwrite:
            if os.path.exists(file_exo):
                os.remove(file_exo)
            lite_conn = sqlite3.connect(file_exo)
            await __copy(pg_conn, lite_conn, settings.DB_2_NAME, is_sample, file_exo)
            __compress(file_exo)
        else:
            if not os.path.exists(file_exo):
                lite_conn = sqlite3.connect(file_exo)
                await __copy(pg_conn, lite_conn, settings.DB_2_NAME, is_sample, file_exo)
            else: print(f"Database file {file_exo} already exists.")
            if not os.path.exists(f"{file_exo}.gz"):
                __compress(file_exo)
            else: print(f"Compressed database file {file_exo} already exists.")


    except sqlite3.OperationalError: 
        print("Seems like you are missing the data/ directory. You must execute the script at the root level of the project.")
        
    except sqlite3.IntegrityError:
        print("The database already exists...")

    except Exception as e:
        print("Some error occured")
        print(e)

    finally:
        if not pg_conn.is_closed():
            await pg_conn.close()
        if lite_conn is not None:
            lite_conn.close()

async def __copy(pg_conn: asyncpg.Connection, lite_conn: sqlite3.Connection, db_name: str, is_sample: bool, file: str):
    cursor = lite_conn.cursor()
    tables = await pg_conn.fetch("""SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public' AND table_type = 'BASE TABLE' AND table_name NOT LIKE 'Map'
    ;""")

    for table in tables:
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
        cursor.execute(create_stmt)

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
                    
                    cursor.execute(insert_stmt, values)

        if not is_sample:
            print(f"Inserting data in table {table_name} from database {db_name}")
            read, write = os.pipe()
            proc1 = await asyncio.create_subprocess_exec(
                "psql",
                "-c", f"""COPY (SELECT * FROM "{table_name}") TO STDOUT CSV""",
                "-U", settings.DB_USERNAME,
                "-d", db_name
            , stdout=write)
            os.close(write)

            proc2 = await asyncio.create_subprocess_exec(
                "sqlite3", file,
                ".mode csv",
                f""".import /dev/stdin {table_name}"""
            , stdin=read, stdout=asyncio.subprocess.PIPE)
            os.close(read)

            assert(proc2.stdout is not None)
            await proc2.stdout.read()
            print(f"Data inserted in table {table_name} from database {db_name} succesfully\n")

    lite_conn.commit()
    print(f"Vaccuming {file} sqlite database")
    lite_conn.execute("VACUUM;")
    lite_conn.commit()
    print("Operation succeeded")


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

import sys
def __usage():
    print("Script that initialises sqlite3 databases by using data stored in the bus2go postgres databases.")
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

    if (sys.argv[1] == "-f" or sys.argv[1] == "--full"):
        asyncio.run(execute(False, overwrite))
    elif (sys.argv[1] == "-s" or sys.argv[1] == "--sample"): 
        asyncio.run(execute(True, overwrite))
