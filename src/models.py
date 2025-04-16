from enum import Enum
from pydantic import BaseModel

class Agency(str, Enum):
    STM = "STM"
    EXO_BUS = "EXO_BUS"
    EXO_TRAIN = "EXO_TRAIN"


class TransitInfo(BaseModel):
    agency: Agency
    route_id: str
    trip_headsign: str
    stop_name: str


class TransitTime(BaseModel):
    transit_info: TransitInfo
    arrival_time: list[int] | str | None


#only tmp
class Response(BaseModel):
    response: list[TransitTime]
