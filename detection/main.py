import sys
sys.path.append('/Users/len/anaconda3/envs/py_projects/python_pro/first_attempt')

import asyncio
import base64
from aiokafka import AIOKafkaConsumer
from ultralytics import YOLO
from detection.detector import detect
from detection.preprocess import preprocess
from config.build import get_config, CONFIG_PATH
from postgres.db import *

async def get_entites(message): 
    return message.value['frame'], message.value['frame_id'], message.value['video_id'], message.value['frame_count']

async def consume() -> None:
    config = await get_config(CONFIG_PATH)

    consumer = AIOKafkaConsumer(
        config.FRAMES_TOPIC,
        bootstrap_servers=f'{config.HOST}:{config.PORT}',
        value_deserializer=lambda m: json.loads(m.decode("utf-8"))
    )
    await consumer.start()

    video_id = ''
    model = YOLO(config.MODEL_PATH, verbose=False)
    async for message in consumer:
        (m_frame, m_frame_id, m_video_id, m_frames) = await get_entites(message) 

        if message.topic == config.FRAMES_TOPIC:
            if video_id != m_video_id:
                video_id = m_video_id
                total_frames = m_frames
                state = await get_state(video_id, config=config)
                state = state[0]['status']
                if state == config.STATES['STOP']:
                    print('skipping processing video with {video_id} id')
                    continue
                else:
                    await update_state(state=config.STATES["INFER"], video_id=video_id, config=config)
                print(f'Video {video_id} is inferencing')
            
            #check if user cancled
            state = await get_state(video_id, config=config)
            state = state[0]['status']
            print(state)
            if state == config.STATES['STOP']:
                print('skipping processing video with {video_id} id')
                continue

            frame, video_id, frame_id = m_frame, m_video_id, m_frame_id
            preprocessed_frame = await preprocess(frame)
            prediction = await detect(preprocessed_frame, model)
            prediction.update({'frame_id': frame_id})
            print('frame_counter: ', frame_id, total_frames)
            if frame_id == total_frames:
                await update_state(state=config.STATES["COMPLETE"], video_id=video_id, config=config)                
            await save_prediction(prediction, video_id, config=config)
        else:
            print('Message is not realted to frame topic')
    await consumer.stop()

if __name__ == "__main__":
    print('Runner started')
    asyncio.run(consume())