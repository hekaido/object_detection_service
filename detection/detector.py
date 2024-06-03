import numpy as np
from typing import Any

async def detect(frame: np.ndarray, model: Any) -> Any:
    result = model(frame)
    if len(result[0].boxes.xyxy) > 0:
        result = {
            "bboxes": result[0].boxes.xyxy.tolist(),
            "confidence": result[0].boxes.conf.tolist(),
            "class_id": result[0].boxes.cls.tolist()
        }
    else:
        result = {
            "bboxes": ["nothing_detected"],
            "confidence": ["nothing_detected"],
            "class_id": ["nothing_detected"]
        }
    return result