#!/usr/bin/env python3

import argparse
import asyncio
import migration
from os import environ

def main():
    parser = argparse.ArgumentParser(description="Script to initialise a postgres database for Bus2Go-backend. By default, downloads all the required static data from the selected agencies.")

    import dotenv
    dotenv.load_dotenv()

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--stm", "-s", action="store_true", help="Initialise the stm database")
    group.add_argument("--exo", "-e", action="store_true", help="Initialise the exo database")
    group.add_argument("--all", "-a", action="store_true", help="Initialise databases for all agencies")
    parser.add_argument("--no-download", "-n", action="store_true", help="Do not download the required files (expects required files to already be downloaded)")

    args = parser.parse_args()

    if args.stm:
        if not args.no_download:
            asyncio.run(migration.download_stm())

        asyncio.run(init_stm())

    elif args.exo:
        if not args.no_download:
            asyncio.run(migration.download_exo())

        asyncio.run(migration.init_database_exo(environ.get("DB_2_NAME", ""), environ.get("DB_USERNAME", ""), environ.get("DB_PASSWORD", ""), int(environ.get("SQLITE_DB_2_VERSION", -1)), int(environ.get("MIN_CLIENT_VERSION_CODE", -1)), int(environ.get("MAX_CLIENT_VERSION_CODE", -1))))

    else:
        if not args.no_download:
            asyncio.run(download_all())

        asyncio.run(init_all())

#TODO refactor inside migration package

async def init_stm() -> bool:
    return await migration.init_database_stm(environ.get("DB_1_NAME", ""), environ.get("DB_USERNAME", ""), environ.get("DB_PASSWORD", ""), int(environ.get("SQLITE_DB_1_VERSION", -1)), int(environ.get("MIN_CLIENT_VERSION_CODE", -1)), int(environ.get("MAX_CLIENT_VERSION_CODE", -1)))

async def download_all():
    await asyncio.gather(migration.download_stm(), migration.download_exo())

async def init_all():
    await asyncio.gather(migration.init_database_stm(environ.get("DB_1_NAME", ""), environ.get("DB_USERNAME", ""), environ.get("DB_PASSWORD", ""), int(environ.get("SQLITE_DB_1_VERSION", -1)), int(environ.get("MIN_CLIENT_VERSION_CODE", -1)), int(environ.get("MAX_CLIENT_VERSION_CODE", -1))), migration.init_database_exo(environ.get("DB_2_NAME", ""), environ.get("DB_USERNAME", ""), environ.get("DB_PASSWORD", ""), int(environ.get("SQLITE_DB_2_VERSION", -1)), int(environ.get("MIN_CLIENT_VERSION_CODE", -1)), int(environ.get("MAX_CLIENT_VERSION_CODE", -1))))

if __name__ == "__main__":
    main()
