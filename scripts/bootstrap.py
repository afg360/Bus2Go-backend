#!/usr/bin/env python3

import sys
import asyncio
import dotenv

from bus2gosettings import agencies

import init_pg_db
import build_sqlite3_dbs

if __name__ == "__main__":
    dotenv.load_dotenv()
    # for now only doing for stm
    # code already checks whether or not stuff exists (should be refactored)
    if not asyncio.run(init_pg_db.migration.download_stm()):
        sys.exit(1)
    
    if not asyncio.run(init_pg_db.init(agency = agencies["stm"])):
        sys.exit(1)

    if not asyncio.run(build_sqlite3_dbs.init_data(agencies["stm"], False)):
        sys.exit(1)

    sys.exit(0)
