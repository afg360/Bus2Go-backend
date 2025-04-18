#!/usr/bin/env python3

import asyncpg
import asyncio
import sqlite3
import sys
from pathlib import Path

src_path = Path(__file__).parents[1]
sys.path.append(str(src_path))

from src import settings

async def execute():
    pg_conn: asyncpg.Connection = await asyncpg.connect(
        database = settings.DB_NAME, 
        user = settings.DB_USERNAME, 
        password = settings.DB_PASSWORD
    )
    lite_conn = sqlite3.connect("data/stm_sample_data.db")
    cursor = lite_conn.cursor()

    try:
        tables = await pg_conn.fetch("""
                    SELECT table_name FROM information_schema.tables 
                    WHERE table_schema = 'public' AND table_type = 'BASE TABLE';
                """)

        for table in tables:
            table_name = table['table_name']
            columns = await pg_conn.fetch("""
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = $1
                ORDER BY ordinal_position;
            """, table_name)

            create_stmt = f"CREATE TABLE IF NOT EXISTS {table_name} (\n"
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
                col_defs.append(f"{col['column_name']} {sqlite_type} {nullable}")

            create_stmt += ",\n".join(col_defs)
            create_stmt += "\n)"
            print(f"Create statement: {create_stmt}")
            cursor.execute(create_stmt)

            rows = await pg_conn.fetch(f"SELECT * FROM {table_name} LIMIT 70000")
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

        lite_conn.commit()
        print("Operation succeeded")

    except sqlite3.OperationalError: 
        print("Seems like you are missing the data/ directory. You must execute the script at the root level of the project.")
        

    except Exception as e:
        print("Some error occured")
        print(e)


    finally:
        await pg_conn.close()
        lite_conn.close()



if __name__ == "__main__":
    print("Building sample database")
    asyncio.run(execute())
