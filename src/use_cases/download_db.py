import os
from typing_extensions import Any
from ..settings import logger
from ..data import get_file_iterator

def get_stm_sample_data() -> dict[str, Any] | None:
    file_name = "stm_sample_data.db.gz"
    logger.info(f"Downloading sample compressed data {file_name}")
    return __get_file_iterator(file_name)

def get_stm_data() -> dict[str, Any] | None:
    file_name = "stm_data.db.gz"
    logger.info(f"Downloading real compressed data {file_name}")
    return __get_file_iterator(file_name)

def get_exo_sample_data() -> dict[str, Any] | None:
    file_name = "exo_sample_data.db.gz"
    logger.info(f"Downloading sample compressed data {file_name}")
    return __get_file_iterator(file_name)

def get_exo_data() -> dict[str, Any] | None:
    file_name = "exo_data.db.gz"
    logger.info(f"Downloading real compressed data {file_name}")
    return __get_file_iterator(file_name)

def __get_file_iterator(file_name: str):
    db_path = f"data/{file_name}"

    if not os.path.exists(db_path):
        logger.critical("File iterator couldn't serve file'")
        return None

    file_iterator = get_file_iterator(db_path)
    headers = {
        "Content-Disposition": f"attachment; filename={file_name}",
        "Content-Length": str(os.path.getsize(db_path)),
        "Cache-Control": "no-cache, no-store, must-revalidate"
    }

    return {
        "content": file_iterator,
        "headers": headers
    }

