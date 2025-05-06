#!/usr/bin/env python3

import argparse
import asyncio
import migration
import sys
from pathlib import Path

src_path = Path(__file__).parents[1]
sys.path.append(str(src_path))
#only import after updating sys.path because we want to ensure src module is found...
from src import settings

def main():
    parser = argparse.ArgumentParser(description="Script to migrate from an sqlite3 database to a postgres database, to better handle concurrency tasks")

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--stm", "-s", action="store_true", help="Download stm data")
    group.add_argument("--exo", "-e", action="store_true", help="Download exo data")
    group.add_argument("--all", "-a", action="store_true", help="Download data from all agencies")

    parser.add_argument("--no-download", "-n", action="store_true", help="Do not download the required files")

    args = parser.parse_args()

    if args.stm:
        if not args.no_download:
            asyncio.run(migration.download_stm())

        asyncio.run(migration.init_database_stm(settings.DB_NAME, settings.DB_USERNAME, settings.DB_PASSWORD))

    elif args.exo:
        if not args.no_download:
            asyncio.run(migration.download_exo())

        asyncio.run(migration.init_database_exo(settings.DB_NAME, settings.DB_USERNAME, settings.DB_PASSWORD))

    else:
        if not args.no_download:
            asyncio.run(__download_all())

        asyncio.run(__init_all())


async def __download_all():
    await asyncio.gather(migration.download_stm(), migration.download_exo())

async def __init_all():
    await asyncio.gather(migration.init_database_stm(settings.DB_NAME, settings.DB_USERNAME, settings.DB_PASSWORD), migration.init_database_exo(settings.DB_NAME, settings.DB_USERNAME, settings.DB_PASSWORD))

if __name__ == "__main__":
    main()
