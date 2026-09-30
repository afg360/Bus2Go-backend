from typing_extensions import Generator
import os

from bus2gosettings.settings import logger

def get_file_iterator(file_path: str) -> Generator[bytes, None, None] | None:
    """Returns an iterator for reading a file in chunks."""
    if not os.path.exists(file_path):
        logger.error(f"The file {file_path} does not exist!")
        return None
    if not os.path.isfile(file_path):
        logger.error("The given path should be a file.")
        return None
    
    with open(file_path, "rb") as f:
        yield from f
