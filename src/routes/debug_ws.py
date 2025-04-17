from fastapi import routing
from fastapi import WebSocket
from pydantic import BaseModel

from ..settings import logger

class Test(BaseModel):
    id : int
    message: str
    lst: list[str]


class Foo(BaseModel):
    response: list[Test]


debug_ws_route = routing.APIRouter(
    prefix = "/api/realtime/debug", tags = ["debug"]
)

@debug_ws_route.websocket("/test")
async def test_web_sockets(websocket: WebSocket):
    logger.info("Accepting websocket for testing")
    await websocket.accept()
    logger.info("Successfully accepted websocket for testing")
    i = 1
    while True:
        data = await websocket.receive_text()
        logger.info(f"Data received: {data}")
        await websocket.send_text(f"Message received. Sending Json")
        lst : list[Test] = []
        #shitty implementation to send {"myarray" : []}
        for j in range(5):
            lst.append(Test(id=i, message="Randomness", lst=["item 1", "item 2", "item 3"]))
            i += 1
        await websocket.send_text(Foo(response=lst).model_dump_json())


@debug_ws_route.websocket("/test/random")
async def get_random(websocket: WebSocket):
    """Testing function to get random data from the database"""
    logger.info("Accepting websocket for random data")
    await websocket.accept()
    logger.info("Successfully accepted websocket for random data")
    await websocket.send_text(Response(response=(await instances["database"].get_random())).model_dump_json())
    logger.info("Closing random test connection")
    await websocket.close()
