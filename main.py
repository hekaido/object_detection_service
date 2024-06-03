import os
import cv2
import json
import base64
import uvicorn
from aiokafka import AIOKafkaProducer
from fastapi import FastAPI, File, UploadFile
from tempfile import NamedTemporaryFile
from typing import Union, Dict
from aiokafka.helpers import create_ssl_context
from config.build import get_config, CONFIG_PATH
from postgres.db import *

app = FastAPI()

producer = None


@app.post("/start")
async def start_app() -> Dict[str, str]:
    try:
        await producer.start()
        return {"message": "App Started"}
    except Exception:
        print(e)
        return {"message": "An error occured during starting app"}

@app.post("/stop")
async def start_app() -> Dict[str, str]:
    try:
        await producer.stop()
        return {"message": "App stopped"}
    except Exception:
        print(e)
        return {"message": "An error occured during stopping app"}

@app.on_event("startup")
async def startup_event() -> None:
    global producer
    producer = AIOKafkaProducer(
        bootstrap_servers=f'localhost:5003',
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    config = await get_config(CONFIG_PATH)

@app.post("/detection/")
async def post_video(file: UploadFile = File(...)) -> Dict[str, Union[str, str]]:
    config = await get_config(CONFIG_PATH)
    temp = NamedTemporaryFile(delete=False)
    video_id = ''
    try:
        try:
            contents = file.file.read()
            with temp as f:
                f.write(contents)
            print('file writed')
        except Exception as ex:
            print(ex)
            return {"message": "There was an error uploading the file"}
        finally:
            file.file.close()

        video_id = await init_state(config.STATES["PROCESSING"], config=config)
        print("processing")

        cap = cv2.VideoCapture(temp.name)
        frame_count = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))

        print(f'Frame count: {frame_count}')
        frame_id = 0
        while cap.isOpened():
            frame_id += 1
            success, frame = cap.read()
            if success:
                _, encoded_frame = cv2.imencode('.jpg', frame)
                encoded_frame = base64.b64encode(encoded_frame.tobytes()).decode('utf-8')
                message = {
                        'frame': encoded_frame,
                        'video_id': str(video_id),
                        'frame_count': frame_count,
                        'frame_id': frame_id
                }
                await producer.send(config.FRAMES_TOPIC, message)
                await asyncio.sleep(1)
            else:
                break
    except Exception as ex:
        print(ex)
        return {"message": "There was an error processing the file"}
    finally:
        os.remove(temp.name)
        return {"id": str(video_id)}

@app.get("/states/{video_id}")
async def get_video_status(video_id: str) -> Dict[str, str]:
    try:
        config = await get_config(CONFIG_PATH)
        result = await get_state(video_id, config=config)
        return {"state": str(result[0]['status'])}
    except ValueError as error:
        return {"error": error}

@app.get("/detection/{video_id}")
async def get_detection_result(video_id: str) -> Dict[str, dict]:
    try:
        config = await get_config(CONFIG_PATH)
        result = await select_inference_result(video_id, config=config)
        return {"result": {id_frame + 1: result[id_frame][0] for id_frame in range(len(result))}}
    except ValueError as error:
        return {"error": error}

@app.post("/stop_detection/{video_id}")
async def stop_detection(video_id: str) -> Dict[str, dict]:
    try:
        config = await get_config(CONFIG_PATH)
        result = await update_state(state=config.STATES["STOP"], video_id=video_id, config=config)
        return {"result": {id_frame + 1: result[id_frame][0] for id_frame in range(len(result))}}
    except ValueError as error:
        return {"error": error}
    
@app.get("/stop_detection/{video_id}")
async def stop_detection(video_id: str) -> Dict[str, dict]:
    try:
        config = await get_config(CONFIG_PATH)
        result = await update_state(state=config.STATES["STOP"], video_id=video_id, config=config)
        return {"result": {id_frame + 1: result[id_frame][0] for id_frame in range(len(result))}}
    except ValueError as error:
        return {"error": error}

if __name__ == '__main__':
    uvicorn.run(app, host='localhost', port=8000)
    try:
        db_engine = get_db_engine()
    except Exception as e:
        print(e)