from fastapi import FastAPI
import asyncio

from .routes.api import download_router
from .routes.ws import ws_route
from .routes.util import util_route
from .use_cases.update_real_time_data import lifespan
from .settings import settings

# add a call where outputs the newer version of the application, perhaps using the github api


app = FastAPI(title = "Bus2Go-realtime",
              version = settings.get_full_version(),
              description = """
              An api that provides realtime data for the Bus2Go mobile application.
              It has the ability to be self-hosted by anyone.
              """, 
              license_info = {
                "name": "GPL-3.0",
                "url": "https://www.gnu.org/licenses/gpl-3.0.en.html",
                },
              lifespan = lifespan
              )

app.include_router(download_router)
app.include_router(ws_route)
app.include_router(util_route)

async def test():
    #lst = await get_data_agency(Agency.STM, "165", "Nord", "Côte-des-Neiges / Mackenzie")
    #print(lst)
    pass

if __name__ == "__main__":
    asyncio.run(test())
