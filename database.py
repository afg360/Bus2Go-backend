import gtfs_realtime_pb2
import asyncpg
import asyncio
import aiohttp
import time
#for testing purposes
import random

from enum import Enum
from pydantic import BaseModel

class Agency(str, Enum):
    STM = "STM"
    EXO = "EXO_OTHER"
    TRAIN = "EXO_TRAIN"


class TransitInfo(BaseModel):
    agency: Agency
    route_id: str
    trip_headsign: str
    stop_name: str


class TransitTime(BaseModel):
    transit_info: TransitInfo
    arrival_time: list[int] | str | None


async def get_new_realtime_data() -> gtfs_realtime_pb2.FeedMessage | int | None:
    tokens = {}
    url = "https://api.stm.info/pub/od/gtfs-rt/ic/v2/tripUpdates"
    with open("./.env", "r") as file:
        for line in file:
            #pdb.set_trace()
            line = line.rsplit()[0]
            info = line.split('=')
            tokens[info[0]] = info[1]
    async with aiohttp.ClientSession() as session:
        try:
            async with await session.get(url, headers={"apiKey": tokens["stm_token"]}) as response:
                if response.status == 200:
                    feed = gtfs_realtime_pb2.FeedMessage()
                    feed.ParseFromString(await response.read())
                    return feed.entity
                else:
                    pass
        except aiohttp.client_exceptions.ClientConnectorError:
            return 1


class Database():
    def __init__(self, connection_pool = None, debug = None):
        self.__connection_pool = connection_pool
        self.__debug = debug

    @classmethod
    async def create(cls, username: str, password: str, debug: bool =  False):
        self = cls()
        self.__connection_pool = await asyncpg.create_pool(
            database="bus2go",
            user=username,
            password=password,
        )
        #await self.__connection_pool.set_client_encoding('UTF8')
        print(self.__connection_pool)
        self.__debug = debug
        return self

    # send a flag to signal last time database has been updated
    async def updateTimes(self) -> None:
        if self.__connection_pool is None:
            raise TypeError
        query = """UPDATE Map SET arrival_time = $1 WHERE 
        trip_id = $2 AND route_id = $3 AND direction_id = $4 
        AND stop_id = $5;
        """
        data = await get_new_realtime_data()
        if data is None:
            raise AssertionError("Error trying to parse the feed message. Aborting")
            #add some log message
            return
        elif data == 1:
            raise TimeoutError()
        # perhaps divide in chunks
        i = 1
        for entity in data: #type: ignore
            chunk = []
            if entity.HasField("trip_update"):
                trip_update = entity.trip_update
                if (trip_update.HasField("trip")):
                    for stop_time in entity.trip_update.stop_time_update:
                        chunk.append((stop_time.arrival.time, 
                                       int(trip_update.trip.trip_id), 
                                       trip_update.trip.route_id, 
                                       trip_update.trip.direction_id, 
                                       stop_time.stop_id))
            async with self.__connection_pool.acquire() as connection: #type: ignore
                async with connection.transaction():
                    await connection.executemany(query, chunk)
                    chunk = []
                    if self.__debug:
                        print(f"Committed chunk#{i}")
                    i += 1

    async def getTime(self, route_id: str, trip_headsign: str, stop_name: str) -> list[int] | None:
        # normal to have so many, a lot of redundancy, since it is a map
        # need to be sure to get the right updated data, may change and then stop depending on stop code used
        time_range = int(time.time()) - 3 * 60# some small range, say 3 minutes
        query = """SELECT arrival_time FROM Map WHERE 
        route_id = $1 AND trip_headsign = $2 AND stop_name = $3 AND arrival_time > $4 ORDER BY arrival_time;
        """
        assert self.__connection_pool is not None
        async with self.__connection_pool.acquire() as connection:
            async with connection.transaction():
                if await self.__exists_route_id(connection, route_id):
                    data = await connection.fetch(query, route_id, trip_headsign, stop_name, time_range)
                    return [x[0] for x in data] if len(data) > 0 else []
                else:
                    return None

    async def get_random(self) -> list[TransitTime]:
        """For testing purposes. Output info from a random bus"""
        length_q = "SELECT COUNT(*) FROM Routes;"
        async with self.__connection_pool.acquire() as connection: #type: ignore
            async with connection.transaction():
                length = int((await connection.fetchrow(length_q))[0])
                lst = []
                for i in range(2):
                    route_id = str(random.randint(1, length))
                    query = "SELECT * FROM Map WHERE route_id = $1"
                    row = await connection.fetchrow(query, route_id)
                    print(row)
                    lst.append(TransitTime(
                        transit_info=TransitInfo(
                            agency=Agency.STM,
                            route_id=row[3],
                            trip_headsign=row[2],
                            stop_name=row[4]
                        ),
                        arrival_time=[row[8]]
                    ))
                return lst


    async def __exists_route_id(self, connection, route_id: str) -> bool:
        query = "SELECT * FROM Map WHERE route_id = $1 LIMIT 1;"
        data = await connection.fetchrow(query, route_id)
        if data is not None:
            return not data == ""
        return False

    async def close(self) -> None:
        assert self.__connection_pool is not None
        await self.__connection_pool.close()


async def test() -> None:
    #get username from environment variables
    database = await Database.create("dabawss", "", True)
    """
    jobs = [
       database.getTime("165", "Sud", "Côte-des-Neiges / Mackenzie"), 
       database.getTime("2435", "2345", "2345"),
       database.getTime("165", "Nord", "Côte-des-Neiges / Mackenzie")
    ]
    """
    #print(await database.getTime("165", "Nord", "Côte-des-Neiges / Mackenzie"))
    #fetching = get_new_realtime_data()
    #results = await asyncio.gather(*jobs)
    #print(results)
    #print(await fetching)
    #await database.close()


if __name__ == "__main__":
    asyncio.run(test())
