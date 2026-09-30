from fastapi import routing

from bus2gosettings.settings import settings

util_route = routing.APIRouter()

#TODO perhaps instead expect some sort of HTTP header to verify authenticity...
@util_route.get("/api/version")
async def api_version() -> str:
    return settings.VERSION + "." + settings.SUB_VERSION
