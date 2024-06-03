import sys
sys.path.append('/Users/len/anaconda3/envs/py_projects/python_pro/first_attempt')

import asyncio
from typing import Any
import asyncpg as pg
from config.build import get_config, CONFIG_PATH


async def get_db_connection() -> Any:
    try:
        config = await get_config(CONFIG_PATH)
        pool = await pg.create_pool(dsn=f'postgresql://{config.DB_USER}:{config.DB_PASSWORD}@{config.HOST}:5432/{config.DB_NAME}')
        conn = await pool.acquire()
        return pool, conn
    except Exception as e:
        print(e)

async def init_table() -> bool:
    try:
        fd = open('postgres/init_tables.sql', 'r')
        sqlFile = fd.read()
        fd.close()
        config = await get_config(CONFIG_PATH)
        print(1)
        pool, conn = await pg.create_pool(dsn=f'postgresql://{config.DB_USER}:{config.DB_PASSWORD}@{config.HOST}:5432/{config.DB_NAME}')
        print(1)
        conn = await conn.acquire()
        async with conn.transaction():
            await conn.execute(sqlFile)
        await conn.close()
        await pool.close()
        return True
    except Exception as e:
        print(e)
        await conn.close()
        await pool.close()
        return False

if __name__ == "__main__":
    print(asyncio.run(init_table()))
