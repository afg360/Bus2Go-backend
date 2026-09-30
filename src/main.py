from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.openapi.docs import get_swagger_ui_html
import asyncio

from bus2gosettings.settings import settings

from .routes.api import download_router
from .routes.ws import ws_route
from .routes.util import util_route
from .use_cases.update_real_time_data import lifespan

# add a call where outputs the newer version of the application, perhaps using the github api


app = FastAPI(title = "Bus2Go-realtime",
              version = settings.get_full_version(),
              description = """
              An api that provides realtime data for the Bus2Go mobile application.
              Can be self-hosted by anyone.
              """, 
              license_info = {
                "name": "GPL-3.0",
                "url": "https://www.gnu.org/licenses/gpl-3.0.en.html",
                },
              #disabled for now so that it doesn't pollute logging for now...
              #lifespan = lifespan
              )

# app.mount("/assets", StaticFiles(directory="assets"), name="assets")
# @app.get("/docs", include_in_schema=False)
# async def swagger_ui_html():
#     return get_swagger_ui_html(
#             openapi_url="/openapi.json",
#             title="FastAPI",
#             swagger_favicon_url="/assets/favicon.ico"
#         )

# @app.get("/", include_in_schema=False)
# async def get_favicon():
#     return FileResponse("./assets/favicon.ico")

app.include_router(download_router)
app.include_router(ws_route)
app.include_router(util_route)


if settings.IS_DEBUG:
    from .routes.debug_api import debug_router
    from .routes.debug_ws import debug_ws_route
    app.include_router(debug_router)
    app.include_router(debug_ws_route)


async def test():
    #lst = await get_data_agency(Agency.STM, "165", "Nord", "Côte-des-Neiges / Mackenzie")
    #print(lst)
    pass

if __name__ == "__main__":
    asyncio.run(test())
