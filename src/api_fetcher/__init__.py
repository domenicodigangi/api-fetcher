import hashlib
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import httpx
import pandas as pd
from api_fetcher.async_task_helper import AsyncTaskHelper
from api_fetcher.cache.redis import RedisCache
from api_fetcher.data_format.dataframe import FormattedDataType, PolarsDataFormatter
from api_fetcher.settings import APISettings
from pydantic import BaseModel

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CachedDataFormat(BaseModel):
    cache_key: str
    data: Optional[FormattedDataType]

    class Config:
        arbitrary_types_allowed = True


class DomotzAPIDataFetcher:
    def __init__(self, api_settings: APISettings, standard_calls=dict, cache=None):
        self.task_helper = AsyncTaskHelper()
        self._api_settings = api_settings
        self._cache = cache or RedisCache(ttl=self._api_settings.cache_ttl)
        self.key_prefix = self.get_key_prefix()
        logger.debug("api_settings: %s", self._api_settings)
        self._start_date_history = self._format_past_datetime(
            self._api_settings.days_history
        )
        self.data_formatter = PolarsDataFormatter()
        self._standard_calls = standard_calls

    def get_key_prefix(self) -> str:
        return hashlib.sha256(self._api_settings.api_key.encode()).hexdigest()

    def _format_past_datetime(self, days_history: int) -> str:
        datetime_from = datetime.utcnow() - timedelta(days=days_history)
        return datetime_from.replace(
            hour=0, minute=0, second=0, microsecond=0
        ).isoformat(timespec="seconds")

    @property
    def standard_calls(self):
        return self._standard_calls

    async def get_iterator(self, item: str, list_path_params: List[Dict]):
        for path_params in list_path_params:
            yield await self.get(item, path_params=path_params)

    async def get(
        self, item: str, path_params: Optional[Dict] = None
    ) -> CachedDataFormat:
        if item in self.standard_calls:
            if path_params is None:
                url = self.standard_calls[item]["path"]
            else:
                url = self.standard_calls[item]["path"].format(**path_params)
            return await self.cached_api_get_formatted(
                url,
                params=self.standard_calls[item]["params"],
            )
        else:
            raise KeyError(f"Item {item} not found in standard calls")

    async def cached_api_get_formatted(
        self, path: str, params: Dict | None = None
    ) -> CachedDataFormat:
        cache_key = self.get_cache_key(path, params)

        cached_data = await self._cache.get(cache_key)

        if cached_data is not None:
            return CachedDataFormat(cache_key=cache_key, data=cached_data)

        result = await self._api_get_formatted(path, params=params)
        await self._cache.set(cache_key, result)
        return CachedDataFormat(cache_key=cache_key, data=result)

    async def _api_get_formatted(
        self, resource_path: str, **kwargs
    ) -> FormattedDataType:
        response = await self._api_get(resource_path, **kwargs)
        return self.data_formatter.format_response(resource_path, response)

    async def _api_get(self, resource_path: str, **kwargs):
        url = f"{self._api_settings.base_url}{resource_path}"
        logger.info("Get from url: %s", url)
        async with httpx.AsyncClient() as client:
            if "params" in kwargs:
                params = kwargs["params"]
            else:
                params = None
            response = await client.get(
                url, headers=self._api_settings.headers, params=params
            )
            response.raise_for_status()

        return response

    def clear_cache(self):
        self._cache.clear_cache()

    def get_cache_key(self, path: str, params: Dict | None = None) -> str:
        return f"{self.key_prefix}{path}{params}"


# if __name__ == "__main__":
#     from api_fetcher.settings import BASE_URLS
#     from dotenv import dotenv_values

#     env = dotenv_values("/workspaces/anomaly-detection-iot/.env")
#     api_fetcher = DomotzAPIDataFetcher(
#         APISettings(api_key=env["API_KEY_EU"], base_url=BASE_URLS["EU"])
#     )
#     api_fetcher.clear_cache()
#     agents = await api_fetcher.get("agents_list")
#     agent_id = agents.data.iloc[0, :]["id"]
#     devices = await api_fetcher.get("list_devices", path_params={"agent_id": agent_id})

#     variables = await api_fetcher.get_all_variables_from_agent(agent_id)
