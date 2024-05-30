import asyncio
from typing import List, Dict, Any, Union, Optional
import json
import ulid
from config.build import get_config, CONFIG_PATH
from postgres.engine import get_db_connection
from postgres.commands import get_command

config = get_config(CONFIG_PATH)

async def init_state(state: str) -> Union[str, Dict[str, str]]:
    """Insert state into the database and return ID."""
    ulid_name = ulid.new()
    db_conn = await get_db_connection()
    command = await get_command(config, state, video_id=ulid_name, action_type='init')
    try:
        tr = db_conn.transaction()
        await tr.start()
        result = await db_conn.execute(command)
        await tr.commit()
        return ulid_name
    except ValueError as error:
        print(error)
        return {"error": error}

async def update_state(state: str, video_id: str) -> Optional[Dict[str, str]]:
    """Update state in the database."""
    db_conn = await get_db_connection()
    command = await get_command(config, state, video_id, action_type='update')
    try:
        await db_conn.execute(command)
        await db_conn.commit()
    except ValueError as error:
        print(error)
        return {"error": error}

async def get_state(video_id: str) -> Union[str, Dict[str, str]]:
    """Get state from the database."""
    db_conn = await get_db_connection()
    command = await get_command(config, video_id=video_id, action_type='get')
    try:
        tr = db_conn.transaction()
        await tr.start()
        result = await db_conn.fetch(command)
        await tr.commit()
        return result
    except ValueError as error:
        print(error)
        return {"error": error}

async def select_inference_result(video_id: str) -> Union[List[object], Dict[str, str]]:
    """Select inference result from the database."""
    db_conn = await get_db_connection()
    command = await get_command(config, video_id=video_id, action_type='inference_result')
    try:
        tr = db_conn.transaction()
        await tr.start()
        result = await db_conn.fetch(command)
        await tr.commit()
        return result
    except ValueError as error:
        print(error)
        return {"error": error}

async def save_prediction(json_data: Dict[str, str], video_id: str) -> Optional[Dict[str, str]]:
    """Save prediction to the database."""
    db_conn = await get_db_connection()
    command = await get_command(config, video_id=video_id, json_data=json_data, action_type='save')
    try:
        tr = db_conn.transaction()
        await tr.start()
        await db_conn.execute(command)
        await tr.commit()
    except ValueError as error:
        print(error)
        return {"error": error}

async def main():
    test_text = 'df'
    id = await init_state(test_text)
    print(id)
    res = await get_state(id)
    res = res[0]['status']
    print(res)
    assert res == test_text

loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)
loop.run_until_complete(main())