import gtfs_realtime_pb2
import sqlite3
import pdb
import asyncio
import requests


def get_new_realtime_data() -> gtfs_realtime_pb2.FeedMessage | None:
    tokens = {}
    url = "https://api.stm.info/pub/od/gtfs-rt/ic/v2/tripUpdates"
    with open("./config", "r") as file:
        for line in file:
            #pdb.set_trace()
            line = line.rsplit()[0]
            info = line.split('=')
            tokens[info[0]] = info[1]
    response = requests.get(url, headers={"apiKey": tokens["stm_token"]})
    if response.status_code == 200:
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(response.content)
        return feed.entity
    else:
        pass


class Database():
    def __init__(self):
        self.__connection = sqlite3.connect("./scripts/stm_info.db")
        self.__connection.execute('PRAGMA encoding = "UTF-8"')

    # send a flag to signal last time database has been updated
    async def updateTimes(self) -> None:
        query = """UPDATE Map SET arrival_time = ? WHERE 
        trip_id = ? AND route_id = ? AND direction_id = ? 
        AND stop_id = ?;
        """
        data = get_new_realtime_data()
        cursor = self.__connection.cursor()
        # perhaps divide in chunks
        i = 1
        for entity in data:
            chunk = []
            if entity.HasField("trip_update"):
                trip_update = entity.trip_update
                if (trip_update.HasField("trip")):
                    cursor.execute("BEGIN TRANSACTION;")
                    for stop_time in entity.trip_update.stop_time_update:
                        chunk.append((stop_time.arrival.time, 
                                       trip_update.trip.trip_id, 
                                       trip_update.trip.route_id, 
                                       trip_update.trip.direction_id, 
                                       stop_time.stop_id))
            cursor.executemany(query, chunk)
            self.__connection.commit()
            chunk = []
            print(f"Committed chunk#{i}")
            i += 1
        cursor.close()

    async def getTime(self, route_id: str, trip_headsign: str, stop_name: str) -> list[int] | None:
        # normal to have so many, a lot of redundancy, since it is a map
        query = """SELECT arrival_time FROM Map WHERE 
        route_id = ? AND trip_headsign = ? AND stop_name = ? AND arrival_time > 0;
        """
        cursor = self.__connection.cursor()
        if self.__exists_route_id(cursor, route_id):
            cursor.execute(query, (route_id, trip_headsign, stop_name))
            data = cursor.fetchall()
            cursor.close()
            return list(map(lambda x: x[0], data)) if len(data) > 0 else list()
        else:
            cursor.close()
            return None

    def __exists_route_id(self, cursor, route_id: str) -> bool:
        query = "SELECT * FROM Map WHERE route_id = ? LIMIT 1;"
        cursor.execute(query, (route_id,))
        data = cursor.fetchone()
        if not data is None:
            return not data == ""
        return False

    def close(self) -> None:
        self.__connection.close()


def main(database: Database) -> None:
    database.updateTimes(feed.entity)


async def test(database: Database) -> None:
    jobs = [
       database.getTime("165", "Sud", "Côte-des-Neiges / Mackenzie"), 
       database.getTime("2435", "2345", "2345"),
       database.getTime("165", "Nord", "Côte-des-Neiges / Mackenzie")
    ]
    #print(get_new_realtime_data())
    results = await asyncio.gather(*jobs)
    print(results)
    database.close()


if __name__ == "__main__":
    #feed = gtfs_realtime_pb2.FeedMessage()
    #with open("./data/stm.txt", "rb") as file:
    #    feed.ParseFromString(file.read())

    database = Database()
    #main(database)
    asyncio.run(test(database))
