from fastapi import routing
from fastapi import WebSocket, WebSocketException, status

from ..models import Response, TransitInfo, TransitTime, Agency
from ..settings import logger, settings
from ..data.database import database

#TODO WHEN SENDING DATA VIA WEBSOCKETS, USE SEND_TEXT, NOT FUCKING JSON, EVEN IF SENDING ACTUAL JSON DATA

ws_route = routing.APIRouter(
    prefix = "/api/realtime/" + settings.VERSION
)

#use the query paramaters from a message
@ws_route.websocket("/")
async def get_data_agency(websocket: WebSocket) -> TransitTime:
    await websocket.accept()
    #TODO handle when client prematurely closes connection
    while True:
        #get query data
        data : list[TransitInfo] = await websocket.receive_json()
        logger.info(f"Input JSON from client: {data}")

        #instead of dealing with the data one by one, deal with it right away and send the whole thing?
        #could also make the operation utilize asynchronous computation
        answer : list[TransitTime] = []
        for transit_info in data:
            #fixme wrong types for the moment for a transit info (since it is in json format)
            item = TransitInfo(agency=Agency(transit_info.agency), route_id=transit_info.route_id, trip_headsign=transit_info.trip_headsign, stop_name=transit_info.stop_name)
            
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
        #FIXME will need to switch that in due time
        response = Response(response=answer)
        print(response)
        await websocket.send_text(response.model_dump_json())

