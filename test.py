#!/bin/python3
import gtfs_realtime_pb2
import sqlite3


conn = sqlite3.connect("./scripts/stm_info.db")
feed = gtfs_realtime_pb2.FeedMessage()
with open("./data/stm.txt", "rb") as file:
    feed.ParseFromString(file.read())

#print(feed.entity[0])
for entity in feed.entity:
    if entity.HasField("trip_update"):
        if (entity.trip_update.HasField("trip")):
            #pdb.set_trace()
            for stop_time in entity.trip_update.stop_time_update:
                if stop_time.stop_id == "56409" and entity.trip_update.trip.route_id == "103" and entity.trip_update.trip.direction_id == 0:
                    print(f"StopId: {stop_time.stop_id}\nRouteId: {entity.trip_update.trip.route_id}\n")
                    print(stop_time)
                    print(entity.trip_update.trip)
