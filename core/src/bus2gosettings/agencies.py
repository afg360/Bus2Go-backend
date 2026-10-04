import tomllib

from pathlib import Path
from pydantic import BaseModel

class AgencyConfig(BaseModel):
    agencyId: str
    name: str
    server_db_name: str
    sqlite_db_name: str
    db_version: int
    api_token: str
    base_api_url: str


def __init_agency_data() -> dict[str, AgencyConfig]:
    with open(Path(__file__).resolve().parent / "agencies.toml", "rb") as file:
        data = tomllib.load(file)
    agencies: dict[str, AgencyConfig] = {}
    for entry in data.get("agencies", []):
        agency = AgencyConfig(**entry)
        agencies[agency.agencyId] = agency

    return agencies

agencies = __init_agency_data()
