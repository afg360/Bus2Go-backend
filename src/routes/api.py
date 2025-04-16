from fastapi import routing

from ..settings import settings

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
    pass

@download_router.get("/exo")
async def download_exo_database():
    """Download the Exo sqlite3 db (containing data for both buses and trains)."""
    pass
