import logging

from cachetools import TTLCache

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())


class InMemoryCache:
    def __init__(self, maxsize=100, ttl=300):
        self.cache = TTLCache(maxsize=maxsize, ttl=ttl)

    async def get(self, key: str):
        data = self.cache.get(key)
        if data is not None:
            logger.info("Found %s in cache", key)
            return data
        logger.info("Not Found %s in cache", key)
        return None

    async def set(self, key: str, value):
        print("SET", key)
        self.cache[key] = value

    async def clear_cache(self):
        logger.info("Clearing in-memory cache")
        self.cache.clear()
