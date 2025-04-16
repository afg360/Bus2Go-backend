from fastapi import routing

from ..settings import settings

util_route = routing.APIRouter(
)

@util_route.get("/api/realtime/version")
async def api_version():
    return {"version": settings.VERSION}
