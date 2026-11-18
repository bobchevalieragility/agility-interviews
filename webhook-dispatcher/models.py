# =====================================================================
# Datamodels
# DO NOT MODIFY THIS FILE
# =====================================================================

import time
from dataclasses import dataclass, field

@dataclass
class Event:
    id: str
    customer_id: str
    payload: dict
    priority: str = "normal"  # "normal" or "high"
    timestamp: float = field(default_factory=time.time)


@dataclass
class HttpResponse:
    status_code: int
    headers: dict[str, str] = field(default_factory=dict)
    body: str = ""


class NetworkTimeoutError(Exception):
    pass
