import numpy as np
from typing import Any

async def detect(frame: np.ndarray, model: Any) -> Any:
    result = model(frame)
    return result