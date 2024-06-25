from fastapi import FastAPI, HTTPException, status
from starlette.responses import JSONResponse
from update import Database
from enum import Enum
from urllib.parse import unquote
import asyncio

import pdb
#may need to log data!!


class Agency(str, Enum):
    STM = "STM"
    EXO = "EXO"
    TRAIN = "TRAIN"


app = FastAPI()


@app.get("/api/realtime/{agency}/")
async def get_data_agency(agency: Agency, route_id: str, trip_headsign: str, stop_name: str):
    try:
        if agency == Agency.STM:
            database = Database()
            stop_name = unquote(stop_name)
            lst = await database.getTime(route_id, trip_headsign, stop_name)
            database.close()
            #pdb.set_trace()
            if lst is None:
                return JSONResponse(status_code=418, content={"message": "The route_id does not exist!", "route_id": route_id, "trip_headsign": trip_headsign, "stop_name": stop_name, "arrival_time": ""})
            elif len(lst) == 0:
                return JSONResponse(status_code=404, content={"message": "No arrival times found", "route_id": route_id, "trip_headsign": trip_headsign, "stop_name": stop_name, "arrival_time": ""})
            return JSONResponse(status_code=200, content={"message": "Successfully found arrival_times", "route_id": route_id, "trip_headsign": trip_headsign, "stop_name": stop_name, "arrival_time": lst})
    except Exception as e:
        #logger.error(f"Exception occurred: {str(e)}")
        return JSONResponse(status_code=500, content={"message": " Something Wrong Happened in the Server", "route_id": route_id, "trip_headsign": trip_headsign, "stop_name": stop_name, "arrival_time": None})


async def test():
    lst = await get_data_agency(Agency.STM, "165", "Nord", "Côte-des-Neiges / Mackenzie")
    print(lst)

if __name__ == "__main__":
    asyncio.run(test())
