from fastapi import FastAPI
import update


"""
Structure;
/api/realtime/{agency}/{bus_num}&{direction}

/api/data/{agency}/{bus_num}
"""


app = FastAPI()


@app.get("/api/realtime/{agency}")
def get_data_agency(agency: str):
    return {"Hello": agency}
