from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from database import Database
from enum import Enum
from urllib.parse import unquote
from contextlib import asynccontextmanager
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import asyncio


class Agency(str, Enum):
    STM = "STM"
    EXO = "EXO"
    TRAIN = "TRAIN"


class TransitInfo(BaseModel):
    agency: Agency
    route_id: str
    trip_headsign: str
    stop_name: str


class TransitTime(BaseModel):
    transit_info: TransitInfo
    arrival_time: list | str | None


import logging
logging.basicConfig(
    level=logging.INFO,  # Set the logging level to INFO or desired level
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)


async def job(database: Database):
    logging.info("Updating database")
    try:
        await database.updateTimes()
        logging.info("Updated database")
    except TypeError:
        logging.error(f"The connection pool is None. Cannot update the database")
    #except Exception as e:
        #logging.error(f"An error occured trying to update database, {e}")


instances = {}


@asynccontextmanager
async def lifespan(app: FastAPI):
    logging.info("Testing start up")
    instances["database"] = await Database.create("dabawss", "")
    scheduler = AsyncIOScheduler()
    scheduler.add_job(job, 'interval', seconds=20, args=[instances["database"]])
    scheduler.start()
    try:
        yield
    finally:
        scheduler.shutdown()
        await instances["database"].close()
        instances.clear()


# add a call where outputs the newer version of the application, perhaps using the github api

VERSION = "v1"
SUB_VERSION = ".0"

app = FastAPI(title="Bus2Go-realtime",
              version=VERSION+SUB_VERSION,
              description="""
              An api that provides realtime data for the Bus2Go mobile application.
              It has the ability to be self-hosted by anyone.
              """, 
              license_info={
                "name": "GPL-3.0",
                "url": "https://www.gnu.org/licenses/gpl-3.0.en.html",
                },
              lifespan=lifespan
              )


@app.get("/api/realtime/"+VERSION+"/version")
async def api_version():
    return {"version": app.version}


@app.get("/api/realtime/"+VERSION+"/")
async def get_data_agency(agency: Agency, route_id: str, trip_headsign: str, stop_name: str) -> TransitTime:
    database = instances["database"]
    item = TransitInfo(agency=agency, route_id=route_id, trip_headsign=trip_headsign, stop_name=unquote(stop_name))
    if item.agency == Agency.STM:
        lst = await database.getTime(item.route_id, item.trip_headsign, item.stop_name)
        #pdb.set_trace()
        if lst is None:
            raise HTTPException(status_code=418, 
                                detail="The route_id does not exist!", 
                                headers={"TransitTime": TransitTime(transit_info=item, arrival_time="").json()}
                                )
        elif len(lst) == 0:
            raise HTTPException(status_code=404, 
                                 detail="No arrival times found",
                                 headers={"TransitTime": TransitTime(transit_info=item, arrival_time="").json()}
                                 )
        return TransitTime(transit_info=item, arrival_time=lst)
    """except Exception as e:
        #logger.error(f"Exception occurred: {str(e)}")
"""


async def test():
    lst = await get_data_agency(Agency.STM, "165", "Nord", "Côte-des-Neiges / Mackenzie")
    print(lst)

if __name__ == "__main__":
    asyncio.run(test())
