import asyncpg
from datetime import datetime

async def _config_table(conn: asyncpg.Connection, version: int, min_client_req_version: int, max_client_req_version: int):
    await conn.execute("""CREATE TABLE IF NOT EXISTS "Config" (
        -- id SERIAL PRIMARY KEY,
        version INTEGER UNIQUE NOT NULL,
        min_client_version_required INTEGER UNIQUE NOT NULL,
        max_client_version_required INTEGER UNIQUE NOT NULL
    );"""
                       )
    print("\nInitialised table Config")
    async with conn.transaction():
        sql = """INSERT INTO "Config"(version, min_client_version_required, max_client_version_required) VALUES($1, $2, $3)"""
        await conn.execute(sql, version, min_client_req_version, max_client_req_version)
    print("Successfully created table Config\n")

def _is_expiry_date_up_to_date(expiry_date: str) -> bool:
    today = datetime.now();
    expiry_date_str = datetime.strptime(str(expiry_date), "%Y%m%d")
    print("Today: ", today, "Expiry date: ", expiry_date)
    return today.date() < expiry_date_str.date()
