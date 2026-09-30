from fastapi import routing
from fastapi import HTTPException
from fastapi.responses import StreamingResponse

from bus2gosettings.settings import settings

from ..use_cases.download_db import transit_agencies

download_router = routing.APIRouter(
    prefix = "/api/download/v" + settings.VERSION
)

# {"min": 2, "max": 6}
@download_router.get("/app-version-code-required")
async def get_app_version_code_required():
    """Get the minimum android app version code needed for the database to work properly"""
    return {
        "min": settings.MIN_CLIENT_VERSION_CODE,
        "max": settings.MAX_CLIENT_VERSION_CODE
    }

@download_router.get("/versions")
async def get_all_databases_versions() -> list[dict[str, str | int]]:
    return  [{ "database": transit_agencies[k].name,
            "version": transit_agencies[k].db_version
            } for k in transit_agencies.keys()]

@download_router.get("/{agency_id}")
async def download_database(agency_id: str):
    """Download the compressed sqlite3 db for the selected agency_id."""
    if agency_id.lower() in transit_agencies.keys():
        response = transit_agencies[agency_id.lower()].get_database()
        if response is None:
            raise HTTPException(status_code = 502, detail="File doesn't exist. Forgot to be init")

        else: 
            return StreamingResponse(
                content = response["content"],
                media_type = "application/gzip",
                headers = response["headers"]
            )
    else:
        raise HTTPException(status_code = 404, detail="This agency does not exist for the moment")

@download_router.get("/{agency_id}/hash")
async def get_stm_hash(agency_id: str) -> str:
    """Get the SHA256 hash of the compressed sqlite3 db for agency_id used for checking database integrity"""
    if agency_id.lower() in transit_agencies.keys():
        try:
            return transit_agencies[agency_id.lower()].get_database_hash()

        except Exception:
            raise HTTPException(status_code = 502, detail="File doesn't exist. Forgot to be init")
    else:
        raise HTTPException(status_code = 404, detail="This agency does not exist for the moment")


@download_router.get("/{agency_id}/version")
async def get_database_version(agency_id: str):
    """Returns the current database version of agency_id"""
    if agency_id.lower() in transit_agencies.keys():
        agency = transit_agencies[agency_id.lower()]
        return {"database": agency.name, "version": agency.db_version}
    else:
        raise HTTPException(status_code = 404, detail="This agency does not exist for the moment")


