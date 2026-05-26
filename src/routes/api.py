from fastapi import routing
from fastapi import HTTPException
from fastapi.responses import StreamingResponse
import asyncio

from ..settings import settings
from ..use_cases.download_db import get_stm_data, get_exo_data, stm_hash_use_case

download_router = routing.APIRouter(
    prefix = "/api/download/" + settings.VERSION
)

#TODO make it return some sort of json with min and max 
# accepted versions
# {"min": 2, "max": 6}
@download_router.get("/app_version_code_required")
async def get_app_version_code_required():
    """Get the minimum android app version code needed for the database to work properly"""
    return {
        "min": settings.MIN_CLIENT_VERSION_CODE,
        "max": settings.MAX_CLIENT_VERSION_CODE
    }

#TODO works for now, but would be a better idea to move this to another section of the code to not forget to change...

@download_router.get("/stm")
async def download_stm_database():
    """Download the STM compressed sqlite3 db."""
    try:
        response = get_stm_data()
        if response is None:
            raise HTTPException(status_code = 502, detail="File doesn't exist. Forgot to be init")

        else: 
            return StreamingResponse(
                content = response["content"],
                media_type = "application/gzip",
                headers = response["headers"]
            )
    except Exception:
        return HTTPException(status_code = 502, detail="File doesn't exist. Forgot to be init")

@download_router.get("/stm/hash")
async def get_stm_hash() -> str:
    """Get the SHA256 hash of the STM compressed sqlite3 db for checking integrity"""
    try:
        return stm_hash_use_case()

    except Exception:
        raise HTTPException(status_code = 502, detail="File doesn't exist. Forgot to be init")

@download_router.get("/stm/version")
async def get_stm_database_version():
    """Returns the current version of the stm database"""
    return {"database": "stm", "version": settings.SQLITE_DB_1_VERSION}

@download_router.get("/exo")
async def download_exo_database():
    """Download the Exo compressed sqlite3 db (containing data for both buses and trains)."""
    try:
        response = get_exo_data()
        if response is None:
            raise HTTPException(status_code = 502, detail="File doesn't exist. Forgot to be init")

        else: 
            return StreamingResponse(
                content = response["content"],
                media_type = "application/gzip",
                headers = response["headers"]
            )
    except Exception:
        raise HTTPException(status_code = 502, detail="File doesn't exist. Forgot to be init")

@download_router.get("/exo/version")
async def get_exo_database_version():
    """Returns the current version of the exo database"""
    return {"database": "exo", "version": settings.SQLITE_DB_2_VERSION}

@download_router.get("/versions")
async def get_all_databases_versions():
    return list(await asyncio.gather(
        get_stm_database_version(),
        get_exo_database_version()
    ))
