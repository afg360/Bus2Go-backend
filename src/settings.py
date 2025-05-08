from pydantic_settings import BaseSettings
import logging
import sys

            
class Settings(BaseSettings):
    VERSION: str = "v1"
    SUB_VERSION: str = ".1"
    PROJECT_NAME: str = "bus2go-realtime"
    LOG_LEVEL: int = logging.DEBUG

    #from .env
    HOST: str
    PORT: int
    SSL_CERT_PATH: str
    SSL_KEY_PATH: str
    STM_TOKEN: str

    EXO_TOKEN: str
    DB_1_NAME: str
    DB_2_NAME: str
    DB_USERNAME: str
    DB_PASSWORD: str
    DEBUG_MODE: bool


    def get_full_version(self) -> str:
        return self.VERSION + self.SUB_VERSION

    def setup_logging(self) -> logging.Logger:
        """Returns a singleton instance of a logger"""
        logger = logging.getLogger()

        if not logger.hasHandlers():
            file_handler = logging.FileHandler(self.PROJECT_NAME + ".log")
            file_handler.setLevel(self.LOG_LEVEL)
            
            file_handler.setFormatter(logging.Formatter(
                '%(asctime)s - %(levelname)s - %(message)s',
                '%Y-%m-%d %H:%M:%S'
            )
)
            logger.setLevel(self.LOG_LEVEL)
            logger.addHandler(file_handler)
            
            if self.LOG_LEVEL == logging.DEBUG:
                console_handler = logging.StreamHandler(sys.stdout)
                console_handler.setLevel(self.LOG_LEVEL)
                console_format = logging.Formatter('%(levelname)s: %(message)s')
                console_handler.setFormatter(console_format)
                logger.addHandler(console_handler)

        return logger

    class Config:
        env_file = ".env"
        case_sensitive = True

settings = Settings() # Tokens setup via the .env configuration

logger = settings.setup_logging()
