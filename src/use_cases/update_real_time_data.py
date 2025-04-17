from fastapi import FastAPI
from contextlib import asynccontextmanager
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from ..data import database
from ..settings import logger, settings


async def job():
    logger.info("Updating database")
    try:
        await database.updateTimes()
        logger.info("Updated database")
    except TypeError:
        logger.error("The connection pool is None. Cannot update the database")
    except TimeoutError:
        logger.error("Timeout of the server trying to gather information. Verify that the server is correctly connected to the internet.")
    #except Exception as e:
        #logger.error(f"An error occured trying to update database, {e}")

@asynccontextmanager
async def lifespan(app : FastAPI):
    logger.info("Testing start up")
    await database.init(settings.DB_NAME, settings.DB_USERNAME, settings.DB_PASSWORD)
    scheduler = AsyncIOScheduler()
    scheduler.add_job(job, 'interval', seconds=20)
    scheduler.start()
    try:
        yield
    finally:
        scheduler.shutdown()
        await database.close()
