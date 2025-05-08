from typing_extensions import Any
from ..settings import logger
from ..data import get_file_iterator

def get_stm_sample_data() -> dict[str, Any] | None:
    """Execute database streaming use case for stm data."""
    file_name = "stm_sample_data.db"
    db_path = f"data/{file_name}"

    logger.info("Downloading sample stm data")

    file_iterator = get_file_iterator(db_path)
    if file_iterator is None:
        logger.critical("File iterator couldn't serve file'")
        return None

    headers = {
        "Content-Disposition": f"attachment; filename={file_name}",
        "Cache-Control": "no-cache, no-store, must-revalidate"
    }

    return {
        "content": file_iterator,
        "headers": headers
    }


def get_exo_sample_data() -> dict[str, Any] | None:
    """Execute database streaming use case for exo data."""
    file_name = "exo_sample_data.db"
    db_path = f"data/{file_name}"

    logger.info("Downloading sample exo data")

    file_iterator = get_file_iterator(db_path)
    if file_iterator is None:
        logger.critical("File iterator couldn't serve file'")
        return None

    headers = {
        "Content-Disposition": f"attachment; filename={file_name}",
        "Cache-Control": "no-cache, no-store, must-revalidate"
    }

    return {
        "content": file_iterator,
        "headers": headers
    }

