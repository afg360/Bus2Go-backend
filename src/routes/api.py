from fastapi import routing
from fastapi import HTTPException
from fastapi.responses import StreamingResponse

from ..settings import settings
from ..use_cases.download_db import get_stm_data, get_exo_data

download_router = routing.APIRouter(
    prefix = "/api/download/" + settings.VERSION
)

@download_router.get("/")
async def download_all_database():
    """Download all the bus2go sqlite3 databases."""
    pass

@download_router.get("/stm")
async def download_stm_database():
    """Download the STM sqlite3 db."""
    response = get_stm_data()
    if response is None:
        return HTTPException(status_code = 403)

    else: 
        return StreamingResponse(
            content = response["content"],
            media_type = "application/octet-stream",
            headers = response["headers"]
        )

@download_router.get("/exo")
async def download_exo_database():
    """Download the Exo sqlite3 db (containing data for both buses and trains)."""
    response = get_exo_data()
    if response is None:
        return HTTPException(status_code = 403)

    else: 
        return StreamingResponse(
            content = response["content"],
            media_type = "application/octet-stream",
            headers = response["headers"]
        )
