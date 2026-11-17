import time
from datetime import datetime, timezone

def now():
    return datetime.now(timezone.utc).isoformat()

def now_epoch():
    return time.time()
