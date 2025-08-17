from __future__ import annotations

import numpy as np
import fireducks.pandas as pd
from typing import Callable
import asyncio
from pathlib import Path
from enum import Enum

from .utils import fetch_dataframe_from_url, fetch_model_from_url, dataframe_from_model
from .game_play_ids import GamePlayIds
from .play import Play, PlayField
from .clip import Clip
from .team import Team
from .broadcast import BroadcastType

class Plays:
    def __init__(self, game_pks: list[int]):
        self._game_pks: list[int] = game_pks
        self._funcs: list[Callable[[pd.DataFrame], pd.DataFrame]] = []
        self._data: pd.DataFrame | None = None
        self._cached = False
        self._other_plays: list[Plays] = []

    async def __load(self) -> pd.DataFrame:
        """Builds a GamePlays instance for the given game_pks."""
        if self._cached and self._data is not None:
            return self._data
        
        data: pd.DataFrame | None = None

        if len(self._game_pks) > 0:
            # Prepare URLs for fetching play IDs and play data
            play_id_urls = [f"https://baseballsavant.mlb.com/gf?game_pk={game_pk}" for game_pk in self._game_pks]
            play_data_url = [f"https://baseballsavant.mlb.com/statcast_search/csv?all=true&type=details&game_pk={game_pk}" for game_pk in self._game_pks]

            # Fetch all play ID models and play dataframes concurrently
            play_id_models = await asyncio.gather(*(fetch_model_from_url(url, GamePlayIds) for url in play_id_urls))
            play_data_dfs = await asyncio.gather(*(fetch_dataframe_from_url(url) for url in play_data_url))

            # Combine play ID dataframes
            play_id_df = dataframe_from_model([play_data for play_id_models in play_id_models for play_data in play_id_models.play_data])

            # Combine play data dataframes
            play_data_dfs = pd.concat(play_data_dfs, ignore_index=True) if play_data_dfs else pd.DataFrame()
            data = play_data_dfs.merge(
                play_id_df,
                left_on=["at_bat_number", "pitch_number", "game_pk"],
                right_on=["ab_number", "pitch_number", "game_pk"],
                how="inner",
                suffixes=("", "_right")
            )

        if self._other_plays:
            other_dataframes = await asyncio.gather(*(plays.__load() for plays in self._other_plays))

            if data is not None:
                data = pd.concat([data] + other_dataframes, ignore_index=True) # type: ignore
            else:
                data = pd.concat(other_dataframes, ignore_index=True) # type: ignore

        if data is None:
            raise ValueError("No data found for the specified game Pks.")

        for func in self._funcs:
            data = func(data)

        self._data = data
        self._cached = True

        return self._data
    
    def filter_for_events(self) -> Plays:
        """Filters the plays to only include those with events."""
        def filter_func(df: pd.DataFrame) -> pd.DataFrame:
            return df[df['events'].notna()] # type: ignore
        
        return self.add(filter_func)
    
    def filter(self, field: PlayField, value: str | int | float | Enum) -> Plays:
        """Filters the plays based on a specific field and value."""
        def filter_func(df: pd.DataFrame) -> pd.DataFrame:
            if isinstance(value, str):
                return df[df[field.value].str.contains(value, na=False)] # type: ignore
            elif isinstance(value, Team):
                return df[df[field.value].str.contains(value.name, na=False)] # type: ignore
            else:
                return df[df[field.value] == value] # type: ignore
            
        return self.add(filter_func)
    
    def sort_by(self, field: PlayField, *, key: Callable[[pd.Series], pd.Series] | None = None, ascending: bool = False) -> Plays:
        """Sorts the plays by a specific field."""
        def sort_func(df: pd.DataFrame) -> pd.DataFrame:
            return df.sort_values(by=field.value, key=key, ascending=ascending) # type: ignore
        
        return self.add(sort_func)
    
    def sort_chronologically(self) -> Plays:
        """Sorts the plays chronologically by game_pk, date, at_bat_number and pitch_number."""
        return self \
            .add(lambda df: pd.DataFrame(df.sort_values(PlayField.PITCH_NUMBER.value))) \
            .add(lambda df: pd.DataFrame(df.sort_values(PlayField.AT_BAT_NUMBER.value))) \
            .add(lambda df: pd.DataFrame(df.sort_values(PlayField.GAME_PK.value))) \
            .add(lambda df: pd.DataFrame(df.sort_values(PlayField.GAME_DATE.value))) \

    def head(self, n: int = 5) -> Plays:
        """Returns the first n plays."""        
        return self.add(lambda df: df.head(n))
    
    def tail(self, n: int = 5) -> Plays:
        """Returns the last n plays."""
        return self.add(lambda df: df.tail(n))

    def add(self, func: Callable[[pd.DataFrame], pd.DataFrame]) -> Plays:
        """Adds a function to the builder to be run on the data frame."""
        result = Plays(self._game_pks)
        result._funcs = self._funcs + [func]
        result._other_plays = [*self._other_plays]
        return result

    async def load(self) -> list[Play]:
        """Returns the game plays as a list of GamePlay."""
        data = await self.__load()
        rows = data.replace({np.nan: None}).to_dict(orient='records')  # type: ignore
        return [Play.model_validate(row) for row in rows]

    async def to_dataframe(self) -> pd.DataFrame:
        """Returns the game plays as a pandas DataFrame."""
        data = await self.__load()
        return data
    
    async def load_clips(self, broadcast: Team | BroadcastType | None) -> list[Clip]:
        """Returns the clips for the plays."""
        plays = await self.load()
        return [Clip(play, broadcast) for play in plays]
    
    async def download_clips(self, path: str | Path, broadcast: Team | BroadcastType | None = None, verbose: bool = False) -> list[Path]:
        """Downloads the clips for the plays."""
        path = Path(path)

        if not path.exists() or not path.is_dir():
            raise ValueError(f"Path {path} does not exist or is not a directory.")

        clips = await self.load_clips(broadcast)
        tasks = [clip.download(path / f'clip_{i}.mp4', verbose) for i, clip in enumerate(clips)]
        return await asyncio.gather(*tasks)

    @property
    def game_pks(self) -> list[int]:
        """Returns the game primary key."""
        return self._game_pks
    
    def __repr__(self) -> str:
        return f"GamePlays(game_pks={self.game_pks})"
    
    def __add__(self, other: Plays) -> Plays:
        result = Plays([])
        result._other_plays = [self, other]
        return result