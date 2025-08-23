from urllib.parse import quote
import redis

from utils.logger import log


class RedisService:
    def __init__(self, host: str, port: int):
        self.redis = redis.Redis(host=host, port=port, decode_responses=True)

    @log
    def set_key(self, key: str, value: object) -> None:
        self.redis.set(key, value)

    @log
    def get_key(self, key: str) -> object:
        return self.redis.get(key)
