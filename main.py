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


@app.get("/")
async def root() -> Dict[str, str]:
    """
    Returns a message indicating the app has started.

    Returns:
        dict: A message indicating the app has started.
    """
    return {"message": "App Started"}

@app.on_event("startup")
async def startup_event() -> None:
    """
    Handles app startup event to instantiate the KafkaProducer.
    """
    global producer
    producer = AIOKafkaProducer(
        bootstrap_servers=f'localhost:5003',
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        #api_version=config.API_VERSION
    )
    await producer.start()
    config = await get_config(CONFIG_PATH)

@app.post("/prediction/")
async def post_video(file: UploadFile = File(...)) -> Dict[str, Union[str, str]]:
    """
    Handles the video upload process.

    Args:
        file (UploadFile): The uploaded video file.

    Returns:
        dict: The upload status message.
    """
    config = await get_config(CONFIG_PATH)
    temp = NamedTemporaryFile(delete=False)
    result = 'no_result'
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

        result = await init_state(config.STATES["PROCESSING"], config=config)
        print("processing")
        cap = cv2.VideoCapture(temp.name)
        fps = cap.get(cv2.CAP_PROP_FPS)
        frame_count = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
        duration = frame_count / fps
        print(f'FPS: {fps}; Frame count: {frame_count}; Duration: {duration}')
        frame_id = 0
        while cap.isOpened():
            frame_id += 1
            success, frame = cap.read()
            if success:
                _, encoded_frame = cv2.imencode('.jpg', frame)
                preprocessed_frame = base64.b64encode(encoded_frame.tobytes()).decode('utf-8')
                data = {'frame': preprocessed_frame,
                        'video_id': str(result),
                        'frame_count': frame_count,
                        'frame_id': frame_id
                        }
                await producer.send(config.FRAMES_TOPIC, data)
                await asyncio.sleep(1)
                print("frame_sent")
            else:
                break
        await producer.stop()
    except Exception as ex:
        print(ex)
        return {"message": "There was an error processing the file"}
    finally:
        os.remove(temp.name)
        return {"id": str(result)}

@app.get("/states/{video_id}")
async def get_video_status(video_id: str) -> Dict[str, str]:
    """
    Retrieves the state of a video by its ID.

    Args:
        video_id (str): The ID of the video.

    Returns:
        dict: The state of the video.
    """
    try:
        config = await get_config(CONFIG_PATH)
        result = await get_state(video_id, config=config)
        return {"state": str(result[0]['status'])}
    except ValueError as error:
        return {"error": error}

@app.get("/prediction/{video_id}")
async def get_inference_result(video_id: str) -> Dict[str, dict]:
    """
    Retrieves the inference result of a video by its ID.

    Args:
        video_id (str): The ID of the video.

    Returns:
        dict: The inference result of the video.
    """
    try:
        config = await get_config(CONFIG_PATH)
        result = await select_inference_result(video_id, config=config)
        return {"result": {id_frame + 1: result[id_frame][0] for id_frame in range(len(result))}}
    except ValueError as error:
        return {"error": error}

@app.post("/stop_prediction/{video_id}")
async def get_inference_result(video_id: str) -> Dict[str, dict]:
    """
    Retrieves the inference result of a video by its ID.

    Args:
        video_id (str): The ID of the video.

    Returns:
        dict: The inference result of the video.
    """
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