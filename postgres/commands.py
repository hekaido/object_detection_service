from config.build import Config
from sqlalchemy import text
import json
async def get_command(config: Config, state=None, video_id=None, json_data=None, action_type: str = '') -> str:
    if action_type == 'init':
        return f"INSERT INTO {config.SHEMA_NAME}.{config.STATE_TABLE} (status, ulid_name) VALUES ('{state}', '{video_id}') RETURNING id"
    elif action_type == 'update':
        return f"UPDATE {config.SHEMA_NAME}.{config.STATE_TABLE} SET status='{state}' WHERE ulid_name='{video_id}'"
    elif action_type == 'get':
        return f"SELECT status FROM {config.SHEMA_NAME}.{config.STATE_TABLE} WHERE ulid_name='{video_id}'"
    elif action_type == 'inference_result':
        return f"SELECT detection_result from {config.SHEMA_NAME}.{config.RESULT_TABLE} WHERE ulid_name='{video_id}'"
    elif action_type == 'save':
        return f"INSERT INTO {config.SHEMA_NAME}.{config.RESULT_TABLE} (detection_result, ulid_name) VALUES ('{json.dumps(json_data)}', '{video_id}')"
    else:
        print(f'Unknow command type: {action_type}')

        