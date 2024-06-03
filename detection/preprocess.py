import cv2
import numpy as np
import base64

async def preprocess(frame: bytes) -> np.ndarray:
    frame_bytes = base64.b64decode(frame)
    frame_array = np.frombuffer(frame_bytes, dtype=np.uint8)
    frame = cv2.imdecode(frame_array, cv2.IMREAD_COLOR)
    frame = cv2.resize(frame, (640, 640))
    return frame