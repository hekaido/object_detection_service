import asyncio
from typing import List, Dict, Any, Union, Optional
import json
import ulid
from config.build import Config
from postgres.engine import get_db_connection
from postgres.commands import get_command

async def init_state(state: str, config: Config) -> Union[str, Dict[str, str]]:
    """Insert state into the database and return ID."""
    ulid_name = ulid.new()
    pool, db_conn = await get_db_connection()
    command = await get_command(config, state, video_id=ulid_name, action_type='init')
    try:
        async with db_conn.transaction():
            result = await db_conn.fetch(command)
        await db_conn.close()
        await pool.close()
        return ulid_name
    except ValueError as error:
        print(error)
        await db_conn.close()
        return {"error": error}

async def update_state(state: str, video_id: str, config: Config) -> Optional[Dict[str, str]]:
    """Update state in the database."""
    pool, db_conn = await get_db_connection()
    command = await get_command(config, state, video_id, action_type='update')
    try:
        async with db_conn.transaction():
            result = await db_conn.fetch(command)
        await db_conn.close()
        await pool.close()
        return result
    except ValueError as error:
        print(error)
        await db_conn.close()
        await pool.close()
        return {"error": error}

async def get_state(video_id: str, config: Config) -> Union[str, Dict[str, str]]:
    """Get state from the database."""
    pool, db_conn = await get_db_connection()
    command = await get_command(config, video_id=video_id, action_type='get')
    try:
        async with db_conn.transaction():
            result = await db_conn.fetch(command)
        await db_conn.close()
        await pool.close()
        return result
    except ValueError as error:
        print(error)
        await db_conn.close()
        await pool.close()
        return {"error": error}

async def select_inference_result(video_id: str, config: Config) -> Union[List[object], Dict[str, str]]:
    """Select inference result from the database."""
    pool, db_conn = await get_db_connection()
    command = await get_command(config, video_id=video_id, action_type='inference_result')
    try:
        async with db_conn.transaction():
            result = await db_conn.fetch(command)
        await db_conn.close()
        await pool.close()
        return result
    except ValueError as error:
        print(error)
        await db_conn.close()
        await pool.close()
        return {"error": error}

async def save_prediction(json_data: Dict[str, str], video_id: str, config: Config) -> Optional[Dict[str, str]]:
    """Save prediction to the database."""
    pool, db_conn = await get_db_connection()
    command = await get_command(config, video_id=video_id, json_data=json_data, action_type='save')
    try:
        async with db_conn.transaction():
            result = await db_conn.fetch(command)
        await db_conn.close()
        await pool.close()
        return result
    except ValueError as error:
        print(error)
        await db_conn.close()
        await pool.close()
        return {"error": error}

async def main():
    test_text = 'df'
    id = await init_state(test_text)
    print(id)
    res = await get_state(id)
    res = res[0]['status']
    print(res)
    assert res == test_text

# loop = asyncio.new_event_loop()
# asyncio.set_event_loop(loop)
# loop.run_until_complete(main())