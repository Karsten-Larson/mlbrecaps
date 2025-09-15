import io
from curl_cffi.requests import Response
from curl_cffi import AsyncSession
import fireducks.pandas as pd
from pydantic import BaseModel
from typing import Type, TypeVar
import asyncio

async def _fetch_url(s: AsyncSession, url: str, retries: int=3) -> Response:
    delay = 1
    for retry in range(retries):
        try:
            response = await s.get(url, timeout=10, impersonate="chrome")
            response.raise_for_status()
            break
        except Exception:
            if retry < retries - 1:
                await asyncio.sleep(delay)  # Add a delay before retrying
                delay *= 2
                continue
            raise

    return response  # type: ignore


async def fetch_url(url: str) -> Response:
    async with AsyncSession() as session:
        return await _fetch_url(session, url)


async def fetch_urls(urls: list[str]) -> list[Response]:
    async with AsyncSession() as session:
        return await asyncio.gather(*[_fetch_url(session, url) for url in urls])


async def _fetch_dataframe_from_url(s: AsyncSession, url: str) -> pd.DataFrame:
    csv = (await _fetch_url(s, url)).content
    return pd.read_csv(io.StringIO(csv.decode('utf-8')))


async def fetch_dataframe_from_url(url: str) -> pd.DataFrame:
    csv = (await fetch_url(url)).content
    return pd.read_csv(io.StringIO(csv.decode('utf-8')))


async def fetch_dataframes_from_urls(url: list[str]) -> list[pd.DataFrame]:
    async with AsyncSession() as s:
        return await asyncio.gather(*[_fetch_dataframe_from_url(s, url) for url in url])


T = TypeVar('T', bound=BaseModel)

async def _fetch_model_from_url(s: AsyncSession, url: str, model: Type[T]) -> T:
    json_data = (await _fetch_url(s, url)).json()
    return model.model_validate(json_data)


async def fetch_model_from_url(url: str, model: Type[T]) -> T:
    json_data = (await fetch_url(url)).json()
    return model.model_validate(json_data)


async def fetch_models_from_url(url: str, model: Type[T]) -> list[T]:
    json_data = (await fetch_url(url)).json()
    return [model.model_validate(item) for item in json_data]


async def fetch_models_from_urls(url: list[str], model: Type[T]) -> list[T]:
    async with AsyncSession() as s:
        return await asyncio.gather(*[_fetch_model_from_url(s, url, model) for url in url])


def dataframe_from_model(data: list[T]) -> pd.DataFrame:
    return pd.DataFrame([item.model_dump() for item in data])


async def fetch_html_from_url(url: str) -> str:
    html = (await fetch_url(url)).content
    return html.decode('utf-8')