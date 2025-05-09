from typing_extensions import Generator
from ..settings import logger
import os

def get_file_iterator(file_path: str) -> Generator[bytes, None, None] | None:
    """Returns an iterator for reading a file in chunks."""
    #FIXME this check doesnt fucking work?
    if not os.path.exists(file_path):
        logger.error(f"The file {file_path} does not exist!")
        return None
    if not os.path.isfile(file_path):
        logger.error("The given path should be a file.")
        return None
    
    logger.info("wtf...")
    with open(file_path, "rb") as f:
        yield from f
