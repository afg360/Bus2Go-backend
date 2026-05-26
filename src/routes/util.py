from fastapi import routing

from ..settings import settings

util_route = routing.APIRouter()

#TODO perhaps instead expect some sort of HTTP header to verify authenticity...
@util_route.get("/api/version")
async def api_version():
    return {"message": "This is a Bus2Go server... potentially", "version": settings.VERSION}
