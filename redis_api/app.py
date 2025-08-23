from fastapi import FastAPI
from fastapi import HTTPException

from services.redis_service import RedisService

app = FastAPI()

redis_host = "redis"
redis_port = 6379
redis = RedisService(redis_host, redis_port)


@app.post("/api/redis/set-key")
def set_key_api(key: str, value: str):
    redis.set_key(key, value)
    return {"message": "Key set successfully", "key": key, "value": value}


@app.get("/api/redis/get-key")
def get_key_api(key: str):
    value = redis.get_key(key)
    if value is None:
        raise HTTPException(status_code=404, detail=f"Key '{key}' not found")
    return {"key": key, "value": value}
