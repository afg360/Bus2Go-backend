# src/routes/debug.py
from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse

from bus2gosettings.settings import settings

from ..use_cases.download_db import transit_agencies

debug_router = APIRouter(
    prefix="/api/debug", tags=["debug"]
)

if settings.IS_DEBUG:
    @debug_router.get("/status")
    async def debug_status():
        return { "status": "debug mode active" }

    @debug_router.get("/sample-data/{agency_id}")
    async def download_sample_data(agency_id: str):
        if agency_id.lower() in transit_agencies.keys():
            response = transit_agencies[agency_id.lower()].get_database()
            if response is None:
                raise HTTPException(status_code = 502, detail="File doesn't exist. Forgot to be init")

            else: 
                return StreamingResponse(
                    content = response["content"],
                    media_type = "application/zstd",
                    headers = response["headers"]
                )
        else:
            raise HTTPException(status_code = 404, detail="This agency does not exist for the moment")


else:
    @debug_router.get("/{path:path}")
    async def debug_not_available(path: str):
        raise HTTPException(status_code=404, detail="Not found")
