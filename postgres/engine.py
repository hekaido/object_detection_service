from typing import Any
import asyncpg as pg
from config.build import get_config, CONFIG_PATH

config = get_config(CONFIG_PATH)

async def get_db_connection() -> Any:
    try:
        conn = await pg.create_pool(dsn=f'postgresql://{config.DB_USER}:{config.DB_PASSWORD}@{config.HOST}:{config.PORT}/{config.DB_NAME}')
        conn = await conn.acquire()
        return conn
    except Exception as e:
        print(e)