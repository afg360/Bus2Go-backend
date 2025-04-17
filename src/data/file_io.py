from typing_extensions import Generator
from ..settings import logger
import os

def get_file_iterator(file_path: str) -> Generator[bytes, None, None] | None:
        """Returns an iterator for reading a file in chunks."""
        if not os.path.exists(file_path):
            # TODO implement mechanism to automatically setup the file?
            logger.critical(f"Not implemented yet. File doesn't exist {file_path}")
        if not os.path.isfile(file_path):
            logger.error("The given path should be a file.")
        
        with open(file_path, "rb") as f:
            yield from f
