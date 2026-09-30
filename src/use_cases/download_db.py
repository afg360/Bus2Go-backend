import os
from typing_extensions import Any

from bus2gosettings.settings import logger, settings
from bus2gosettings.agencies import AgencyConfig, agencies

from ..data import get_file_iterator

class TransitAgency():
    def __init__(self, agencyConfig: AgencyConfig) -> None:
        self.id = agencyConfig.agencyId
        self.name = agencyConfig.name
        self.db_version = agencyConfig.db_version
        self.file_name = f"{self.id}_data_{self.db_version}.db.gz"
        self.api_key = os.getenv(agencyConfig.api_token)
        if settings.IS_DEBUG:
            self.sample_file_name = f"{self.id}_sample_data_{self.db_version}.db.gz"


    def get_database(self) -> dict[str, Any] | None:
        db_path = f"data/{self.file_name}"
        logger.info(f"Downloading real compressed data {self.file_name}")
        return self.__get_database(db_path)


    if settings.IS_DEBUG:
        def get_sample_database(self) -> dict[str, Any] | None:
            db_path = f"data/{self.sample_file_name}"
            logger.info(f"Downloading sample compressed data {self.file_name}")
            return self.__get_database(db_path)


    def get_database_hash(self) -> str:
        file_name = f"data/{self.file_name}.txt"
        logger.info(f"Retrieving {self.name} hash checksum")
        with open(file_name, "r") as file:
            return file.read(-1)


    def __get_database(self, db_path: str) -> dict[str, Any] | None:
        if not os.path.exists(db_path):
            logger.critical("File iterator couldn't serve file'")
            return None

        file_iterator = get_file_iterator(db_path)
        headers = {
            "Content-Disposition": f"attachment; filename={self.file_name}",
            "Content-Length": str(os.path.getsize(db_path)),
            "Cache-Control": "no-cache, no-store, must-revalidate"
        }

        return {
            "content": file_iterator,
            "headers": headers
        }


transit_agencies = { k: TransitAgency(agencies[k]) for k in agencies}
