from fastapi import FastAPI, HTTPException
from update import Database
from enum import Enum
from urllib.parse import unquote


"""
Structure;
/api/realtime/{agency}/{bus_num}&{direction}

/api/data/{agency}/{bus_num}
"""


class Agency(str, Enum):
    stm = "stm"
    exo = "exo"
    train = "train"


app = FastAPI()


@app.get("/api/realtime/{agency}/")
def get_data_agency(agency: Agency, route_id: str, trip_headsign: str, stop_name: str):
    if agency == Agency.stm:
        database = Database()
        stop_name = unquote(stop_name)
        lst = database.getTime(route_id, trip_headsign, stop_name)
        print(lst)
        if lst is None or len(lst) == 0:
            raise HTTPException(status_code=404, 
                                detail="The requested route_id does not seem to exist.")
            
        return {"agency": agency, "route_id": route_id, "trip_headsign": trip_headsign, "stop_name": stop_name, "arrival_time": lst[0][0]}
