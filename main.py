from fastapi import FastAPI, HTTPException, WebSocket, WebSocketException, WebSocketDisconnect, status
from pydantic import BaseModel
from database import Database, Agency, TransitInfo, TransitTime
from contextlib import asynccontextmanager
from apscheduler.schedulers.asyncio import AsyncIOScheduler
#import pdb
import asyncio

#TODO WHEN SENDING DATA VIA WEBSOCKETS, USE SEND_TEXT, NOT FUCKING JSON, EVEN IF SENDING ACTUAL JSON DATA

import logging
logging.basicConfig(
    filename="bus2go-backend.log",
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)


async def job(database: Database):
    logging.info("Updating database")
    try:
        await database.updateTimes()
        logging.info("Updated database")
    except TypeError:
        logging.error("The connection pool is None. Cannot update the database")
    except TimeoutError:
        logging.error("Timeout of the server trying to gather information. Verify that the server is correctly connected to the internet.")
    #except Exception as e:
        #logging.error(f"An error occured trying to update database, {e}")


instances: dict[str, Database] = {}


@asynccontextmanager
async def lifespan(app : FastAPI):
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

@app.get("/api/realtime/version")
async def api_version():
    return {"version": app.version}

class Test(BaseModel):
    id : int
    message: str
    lst: list[str]


class Foo(BaseModel):
    response: list[Test]

@app.websocket("/api/realtime/"+VERSION+"/test")
async def test_web_sockets(websocket: WebSocket):
    logging.info("Accepting websocket for testing")
    await websocket.accept()
    logging.info("Successfully accepted websocket for testing")
    i = 1
    while True:
        data = await websocket.receive_text()
        logging.info(f"Data received: {data}")
        await websocket.send_text(f"Message received. Sending Json")
        lst : list[Test] = []
        #shitty implementation to send {"myarray" : []}
        for j in range(5):
            lst.append(Test(id=i, message="Randomness", lst=["item 1", "item 2", "item 3"]))
            i += 1
        await websocket.send_text(Foo(response=lst).model_dump_json())


@app.websocket("/api/realtime/"+VERSION+"/test/random")
async def get_random(websocket: WebSocket):
    """Testing function to get random data from the database"""
    logging.info("Accepting websocket for random data")
    await websocket.accept()
    logging.info("Successfully accepted websocket for random data")
    await websocket.send_text(Response(response=(await instances["database"].get_random())).model_dump_json())
    logging.info("Closing random test connection")
    await websocket.close()


#only tmp
class Response(BaseModel):
    response: list[TransitTime]

#use the query paramaters from a message
@app.websocket("/api/realtime/"+VERSION+"/")
async def get_data_agency(websocket: WebSocket) -> TransitTime:
    await websocket.accept()
    database = instances["database"]
    #TODO handle when client prematurely closes connection
    while True:
        #get query data
        data : list[TransitInfo] = await websocket.receive_json()
        logging.info(f"Input JSON from client: {data}")

        #instead of dealing with the data one by one, deal with it right away and send the whole thing?
        #could also make the operation utilize asynchronous computation
        answer : list[TransitTime] = []
        for transit_info in data:
            #fixme wrong types for the moment for a transit info (since it is in json format)
            item = TransitInfo(agency=Agency(transit_info["agency"]), route_id=transit_info["route_id"], trip_headsign=transit_info["trip_headsign"], stop_name=transit_info["stop_name"])
            
            if item.agency == Agency.STM:
                lst = await database.getTime(item.route_id, item.trip_headsign, item.stop_name)
                if lst is None:
                    raise WebSocketException(code=status.WS_1003_UNSUPPORTED_DATA)
                elif len(lst) == 0:
                    #raise WebSocketException(code=status.WS_1003_UNSUPPORTED_DATA)
                    answer.append(TransitTime(transit_info=item, arrival_time=[]))
                else:
                    answer.append(TransitTime(transit_info=item, arrival_time=lst))
            #close the connection if no data has been received for a while (e.g. 1 minute, 30 seconds, or wtv)

            else:
                #not implemented yet
                raise WebSocketException(code=status.WS_1011_INTERNAL_ERROR, reason="Not implemented for this agency yet")
        #fixme will need to switch that in due time
        response = Response(response=answer)
        print(response)
        await websocket.send_text(response.model_dump_json())



#will need to have some more entries to download the right data
@app.get("/api/download/"+VERSION+"/")
async def download_database() -> TransitTime:
    """Download the requested data to create the database in the client application"""
    pass


async def test():
    #lst = await get_data_agency(Agency.STM, "165", "Nord", "Côte-des-Neiges / Mackenzie")
    #print(lst)
    pass

if __name__ == "__main__":
    asyncio.run(test())
