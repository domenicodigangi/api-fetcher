import logging
import pickle

from redis import asyncio as aioredis
import redis

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())


class RedisCache:
    def __init__(self, url="redis://localhost:6379", ttl: int = 300):
        self._url = url
        self._ttl = ttl

    async def get(self, key: str):
        async with aioredis.from_url(self._url) as client:
            data = await client.get(key)
            if data is not None:
                logger.info("Found %s in Redis", key)
                return pickle.loads(data)
            logger.info("Not Found %s in Redis", key)
            return None

    async def set(self, key: str, value):
        async with aioredis.from_url(self._url) as client:
            await client.setex(key, self._ttl, pickle.dumps(value))

    def clear_cache(self):
        logger.info("Clearing Redis cache")
        r = redis.StrictRedis.from_url(self._url)
        r.flushdb()
