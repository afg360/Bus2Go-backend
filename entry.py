from src.settings import settings
import uvicorn

if __name__ == "__main__":
    if settings.DEBUG_MODE:
        uvicorn.run(
            "src.main:app",
            host=settings.HOST,
            port=settings.PORT,
        )
    else:
        uvicorn.run(
            "src.main:app",
            host=settings.HOST,
            port=settings.PORT,
            ssl_keyfile=settings.SSL_KEY_PATH,
            ssl_certfile=settings.SSL_CERT_PATH
        )
