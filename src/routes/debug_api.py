# src/routes/debug.py
from fastapi import APIRouter, HTTPException

from ..settings import settings

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
        pass

    @debug_router.get("/sample_data/exo")
    async def download_exo_sample_data():
        pass

else:
    @debug_router.get("/{path:path}")
    async def debug_not_available(path: str):
        raise HTTPException(status_code=404, detail="Not found")
