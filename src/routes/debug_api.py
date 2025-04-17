# src/routes/debug.py
from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse

from ..settings import settings
from ..use_cases.download_db import get_stm_sample_data

debug_router = APIRouter(
    prefix="/api/debug", tags=["debug"]
)

if settings.DEBUG_MODE:
    @debug_router.get("/status")
    async def debug_status():
        return { "status": "debug mode active" }
    
    #test with the client to download a few kbs instead of the whole databases
    @debug_router.get("/sample_data")
    async def download_all_sample_data():
        pass

    @debug_router.get("/sample_data/stm")
    async def download_stm_sample_data():
        response = get_stm_sample_data()
        if response is None:
            return HTTPException(status_code = 403)

        else: 
            return StreamingResponse(
                content = response["content"],
                media_type = "application/octet-stream",
                headers = response["headers"]
            )


    @debug_router.get("/sample_data/exo")
    async def download_exo_sample_data():
        pass

else:
    @debug_router.get("/{path:path}")
    async def debug_not_available(path: str):
        raise HTTPException(status_code=404, detail="Not found")
