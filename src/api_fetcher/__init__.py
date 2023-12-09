import hashlib
import logging
from datetime import datetime, timedelta
from typing import Dict, Optional, Tuple

import httpx
import pandas as pd
from api_fetcher.async_task_helper import AsyncTaskHelper
from api_fetcher.cache.redis import RedisCache
from api_fetcher.data_formatter import FormattedDataType, PandasDataFormatter
from api_fetcher.settings import DomotzAPISettings
from pydantic import BaseModel

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CachedDataFormat(BaseModel):
    cache_key: str
    data: Optional[FormattedDataType]

    class Config:
        arbitrary_types_allowed = True


class DomotzAPIDataFetcher:
    def __init__(self, api_settings: DomotzAPISettings, cache=None):
        self.task_helper = AsyncTaskHelper()
        self._api_settings = api_settings
        self._cache = cache or RedisCache(ttl=self._api_settings.cache_ttl)
        self.key_prefix = self.get_key_prefix()
        logger.debug("api_settings: %s", self._api_settings)
        self._start_date_history = self._format_past_datetime(
            self._api_settings.days_history
        )
        self.data_formatter = PandasDataFormatter()

    def get_key_prefix(self) -> str:
        return hashlib.sha256(self._api_settings.api_key.encode()).hexdigest()

    def _format_past_datetime(self, days_history: int) -> str:
        datetime_from = datetime.utcnow() - timedelta(days=days_history)
        return datetime_from.replace(
            hour=0, minute=0, second=0, microsecond=0
        ).isoformat(timespec="seconds")

    @property
    def _standard_calls(self):
        return {
            "agents_list": {"path": "/agent", "params": {}},
            "agent": {"path": "/agent/{agent_id}", "params": {}},
            "agent_status_history": {
                "path": "/agent/{agent_id}/history/network/event",
                "params": {},
            },
            "list_devices": {
                "path": "/agent/{agent_id}/device",
                "params": {"show_hidden": True},
            },
            "list_device_variables": {
                "path": "/agent/{agent_id}/device/variable",
                "params": {"page_size": 1000, "has_history": "true"},
            },
            "device_inventory": {
                "path": "/agent/{agent_id}/device/{device_id}/inventory",
                "params": {},
            },
            "history_device_variable": {
                "path": "/agent/{agent_id}/device/{device_id}/variable/{variable_id}/history",
                "params": {"from": self._start_date_history},
            },
        }

    async def get(self, item: str, path_params: Optional[Dict] = None):
        if item in self._standard_calls:
            if path_params is None:
                url = self._standard_calls[item]["path"]
            else:
                url = self._standard_calls[item]["path"].format(**path_params)
            return await self.cached_api_get_formatted(
                url,
                params=self._standard_calls[item]["params"],
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
        url = f"{self._api_settings.public_api_url}{resource_path}"
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

    async def get_all_variables_from_agent(
        self, agent_id: int
    ) -> Tuple[FormattedDataType, dict[str, dict]]:
        async def get_history_device_variable(
            agent_id: int, device_id: int, variable_id: int
        ) -> CachedDataFormat:
            return await self.get(
                "history_device_variable",
                path_params={
                    "agent_id": agent_id,
                    "device_id": device_id,
                    "variable_id": variable_id,
                },
            )

        variables = await self.get("list_device_variables", {"agent_id": agent_id})
        variables_history = {}
        df_variables = variables.data
        df_variables = df_variables.loc[df_variables["has_history"], :]
        df_variables["history_hash"] = None
        df_variables["cache_key"] = None

        task_res = await self.task_helper.define_and_gather_task(
            get_history_device_variable,
            [
                (agent_id, row["device_id"], row["id"])
                for ind, row in df_variables.iterrows()
            ],
            args_to_ret_inds=[2],
        )

        for safe_res in task_res:
            (
                success,
                res,
                args_retuned,
                kwargs_retuned,
            ) = safe_res
            variable_id = args_retuned[0]
            var_ind = df_variables["id"] == variable_id
            if success and res is not None:
                df_history = res.data
                variables_history[res.cache_key] = {
                    "hist": df_history,
                }
                df_variables.loc[var_ind, "history_hash"] = hash(df_history.to_json())
                df_variables.loc[var_ind, "cache_key"] = res.cache_key
                df_variables.loc[var_ind, "history_len"] = df_history.shape[0]
            else:
                df_variables.loc[var_ind, "has_history"] = False

        df_variables = (
            df_variables.loc[df_variables["has_history"], :]
            .groupby(by=["history_hash", "path"])
            .agg(
                cache_key=("cache_key", "first"),
                device_id=("device_id", "first"),
                id=("id", "first"),
                replication_count=("id", "count"),
                metric=("metric", "first"),
                unit=("unit", "first"),
            )
            .reset_index()
        )
        variables_history = {
            k: v
            for k, v in variables_history.items()
            if k in df_variables["cache_key"].tolist()
        }
        return df_variables, variables_history


# if __name__ == "__main__":
#     from api_fetcher.settings import BASE_URLS
#     from dotenv import dotenv_values

#     env = dotenv_values("/workspaces/anomaly-detection-iot/.env")
#     api_fetcher = DomotzAPIDataFetcher(
#         DomotzAPISettings(api_key=env["API_KEY_EU"], base_url=BASE_URLS["EU"])
#     )
#     api_fetcher.clear_cache()
#     agents = await api_fetcher.get("agents_list")
#     agent_id = agents.data.iloc[0, :]["id"]
#     devices = await api_fetcher.get("list_devices", path_params={"agent_id": agent_id})

#     variables = await api_fetcher.get_all_variables_from_agent(agent_id)
