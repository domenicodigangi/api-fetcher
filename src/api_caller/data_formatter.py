import logging

import httpx
import pandas as pd

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
FormattedDataType = pd.DataFrame


class PandasDataFormatter:
    def format_response(self, url: str, response: httpx.Response) -> FormattedDataType:
        r_json_list = response.json()
        logger.debug("r_json_list: %s", r_json_list)
        df = pd.json_normalize(r_json_list)
        self._format_df(url, df)
        return df

    def _format_df(self, url: str, df: FormattedDataType) -> FormattedDataType:
        if "timestamp" in df.columns:
            df["timestamp"] = pd.to_datetime(df["timestamp"])
        self._cast_col_if_present(df, "type", "category")
        self._cast_col_if_present(df, "has_history", "bool")
        self._cast_col_if_present(df, "path", "string")
        self._cast_col_if_present(df, "metric", "string")
        self._cast_col_if_present(df, "unit", "string")
        if "history" == url.split("/")[-1]:
            self._cast_col_if_present(df, "value", "float")

        return df

    def _cast_col_if_present(self, df: FormattedDataType, col_name: str, col_type: str):
        if col_name in df.columns:
            df[col_name] = df[col_name].astype(col_type)
