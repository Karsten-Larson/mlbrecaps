import io
from curl_cffi.requests import Response
from curl_cffi import AsyncSession
import fireducks.pandas as pd
from pydantic import BaseModel
from typing import Type, TypeVar

async def fetch_url(url: str) -> Response:
    async with AsyncSession() as s:
        for retry in range(3):
            try:
                response = await s.get(url, timeout=10, impersonate="chrome")
                response.raise_for_status()
                break
            except Exception:
                if retry < 2:
                    continue
                raise

    return response # type: ignore

async def fetch_dataframe_from_url(url: str) -> pd.DataFrame:
    csv = (await fetch_url(url)).content
    return pd.read_csv(io.StringIO(csv.decode('utf-8')))


T = TypeVar('T', bound=BaseModel)

async def fetch_model_from_url(url: str, model: Type[T]) -> T:
    json_data = (await fetch_url(url)).json()
    return model.model_validate(json_data)

def dataframe_from_model(data: list[T]) -> pd.DataFrame:
    return pd.DataFrame([item.model_dump() for item in data])

async def fetch_html_from_url(url: str) -> str:
    html = (await fetch_url(url)).content
    return html.decode('utf-8')