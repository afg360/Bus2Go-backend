#!/bin/python3
import gtfs_realtime_pb2
import pdb

feed = gtfs_realtime_pb2.FeedMessage()
with open("./data/stm.txt", "rb") as file:
    feed.ParseFromString(file.read())

#print(feed.entity[0])
for entity in feed.entity:
    if entity.HasField("trip_update"):
        if (entity.trip_update.HasField("trip")):
            #pdb.set_trace()
            for stop_time in entity.trip_update.stop_time_update:
                if (stop_time.stop_id == "56409" and entity.trip_update.trip.route_id == "103"):
                    print(stop_time)

