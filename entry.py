#!/usr/bin/env python3

from bus2gosettings import settings

import uvicorn

if __name__ == "__main__":
    if settings.IS_DEBUG:
        uvicorn.run(
            "src.main:app",
            host=settings.HOST,
            port=80,
            #port=settings.PORT,
        )
    else:
        uvicorn.run(
            "src.main:app",
            host=settings.HOST,
            port=80,
            #port=settings.PORT,
            ssl_keyfile=settings.SSL_KEY_PATH,
            ssl_certfile=settings.SSL_CERT_PATH
        )
