import logging

from cachetools import LFUCache

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())


class InMemoryCache:
    _instance = None

    def __init__(self, maxsize=128):
        self.cache = LFUCache(maxsize=maxsize)

    @classmethod
    def get_instance(cls, maxsize=128):
        if cls._instance is None:
            cls._instance = cls(maxsize=maxsize)
        elif maxsize != cls._instance.cache.maxsize:
            raise ValueError(
                "InMemoryCache already instantiated with a different maxsize"
            )
        return cls._instance

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
