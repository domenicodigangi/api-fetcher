from pydantic import Field
from pydantic_settings import BaseSettings

BASE_URLS = {
    "EU": "https://api-eu-west-1-cell-1.domotz.com",
    "US": "https://api-us-east-1-cell-1.domotz.com",
}


class DomotzAPISettings(BaseSettings):
    api_key: str | None = Field(None, env="API_KEY")
    base_url: str = Field(default=BASE_URLS["US"], env="BASE_URL")
    cache_ttl: int = Field(default=3600, env="CACHE_TTL")
    days_history: int = Field(default=14, env="DAYS_HISTORY")

    class Config:
        env_file = "/workspaces/domotz-dashboards/.env"

    @property
    def headers(self):
        return {"Accept": "application/json", "X-Api-Key": self.api_key}

    @property
    def public_api_url(self):
        return f"{self.base_url}/public-api/v1"

    def usable_api_key(self) -> bool:
        if self.api_key not in ["", None]:
            if len(self.api_key) >= 32:
                return True
        else:
            return False
