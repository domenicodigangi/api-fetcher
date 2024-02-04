import unittest

from api_fetcher.cache.memory import InMemoryCache


class TestInMemoryCache(unittest.TestCase):
    def setUp(self):
        self.cache = InMemoryCache.get_instance(maxsize=2)

    def test_get_instance(self):
        self.assertIsInstance(self.cache, InMemoryCache)

    async def test_get(self):
        await self.cache.set("test_key", "test_value")
        self.assertEqual(await self.cache.get("test_key"), "test_value")

    async def test_get_non_existent_key(self):
        self.assertIsNone(await self.cache.get("non_existent_key"))

    async def test_set(self):
        await self.cache.set("test_key", "test_value")
        self.assertEqual(await self.cache.get("test_key"), "test_value")

    async def test_clear_cache(self):
        await self.cache.set("test_key", "test_value")
        await self.cache.clear_cache()
        self.assertIsNone(await self.cache.get("test_key"))

    async def test_update_value(self):
        await self.cache.set("test_key", "test_value")
        await self.cache.set("test_key", "new_test_value")
        self.assertEqual(await self.cache.get("test_key"), "new_test_value")

    def test_get_instance_with_different_maxsize(self):
        with self.assertRaises(ValueError):
            InMemoryCache.get_instance(maxsize=3)

    def test_get_instance_same_maxsize(self):
        same_cache = InMemoryCache.get_instance(maxsize=2)
        self.assertEqual(self.cache, same_cache)


if __name__ == "__main__":
    unittest.main()
