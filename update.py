##!/bin/python3
import gtfs_realtime_pb2
import sqlite3


class Database():
    def __init__(self):
        self.__connection = sqlite3.connect("./scripts/stm_info.db")
        self.__connection.execute('PRAGMA encoding = "UTF-8"')

    def updateTimes(self, data: list[gtfs_realtime_pb2.FeedEntity]) -> None:
        query = """UPDATE Map SET arrival_time = ? WHERE 
        trip_id = ? AND route_id = ? AND direction_id = ? 
        AND stop_id = ?;
        """
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

    def getTime(self, route_id: str, trip_headsign: str, stop_name: str) -> list[tuple[int]]:
        # normal to have so many, a lot of redundancy, since it is a map
        query = """SELECT arrival_time FROM Map WHERE 
        route_id = ? AND trip_headsign = ? AND stop_name = ?;
        """
        cursor = self.__connection.cursor()
        cursor.execute(query)
        data = cursor.fetchall()
        cursor.close()
        return data

    def close(self) -> None:
        self.__connection.close()


if __name__ == "__main__":
    feed = gtfs_realtime_pb2.FeedMessage()
    with open("./data/stm.txt", "rb") as file:
        feed.ParseFromString(file.read())

    database = Database()
    database.updateTimes(feed.entity)
    database.close()
