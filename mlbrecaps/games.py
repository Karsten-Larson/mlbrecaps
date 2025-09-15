from __future__ import annotations

from typing import Iterator
from pydantic import BaseModel, ConfigDict
from functools import cached_property

from .date import Date
from .plays import Plays
from .game import Game
from .team import Team
from .utils import fetch_model_from_url

class GameDate(BaseModel):
    model_config = ConfigDict(frozen=True)
    date: str
    totalGames: int
    totalGamesInProgress: int
    games: list[Game]

    @property
    def final_games(self) -> list[Game]:
        """Returns a list of final games for the date."""
        return [game for game in self.games if game.is_valid_game]


class Games(BaseModel):
    model_config = ConfigDict(frozen=True)
    totalGames: int
    totalGamesInProgress: int
    dates: list[GameDate]

    @cached_property
    def game_pks(self) -> set[int]:
        """Returns a list of game Pks."""
        return {game.gamePk for date in self.dates for game in date.final_games}

    @cached_property
    def games_by_pk(self) -> dict[int, Game]:
        """Returns a dictionary mapping game Pks to Game objects."""
        return {game.gamePk: game for date in self.dates for game in date.final_games}

    @cached_property
    def games_by_date(self) -> dict[str, list[Game]]:
        """Returns a dictionary mapping dates to lists of Game objects."""
        return {date.date: date.final_games for date in self.dates}
    
    @cached_property
    def games(self) -> list[Game]:
        """Returns a flat list of all Game objects."""
        return [game for date in self.dates for game in date.final_games]
        
    @cached_property
    def plays(self) -> Plays:
        return Plays(self.game_pks)

    @cached_property
    def games_by_team(self) -> dict[int, list[Game]]:
        """Returns a dictionary mapping team IDs to lists of Game objects."""
        team_games = {}
        for date in self.dates:
            for game in date.final_games:
                team_id = game.teams.away.team.id
                if team_id not in team_games:
                    team_games[team_id] = []
                team_games[team_id].append(game)
                team_id = game.teams.home.team.id
                if team_id not in team_games:
                    team_games[team_id] = []
                team_games[team_id].append(game)
        return team_games  
    
    def iter_games(self) -> Iterator[Game]:
        """Returns an iterator over all Game objects."""
        return iter(self.games)

    def __len__(self) -> int:
        """Returns the total number of games."""
        return len(self.games)  
    
    def __add__(self, other: Games) -> Games:
        return Games(
            totalGames=self.totalGames + other.totalGames,
            totalGamesInProgress=self.totalGamesInProgress + other.totalGamesInProgress,
            dates=self.dates + other.dates
        )

    @staticmethod
    async def get_games(date: Date) -> Games:
        url = f'https://statsapi.mlb.com/api/v1/schedule?startDate={date.start_date}&endDate={date.end_date}&sportId=1'

        return await fetch_model_from_url(url, Games)

    @staticmethod
    async def get_games_by_team(team: Team, date: Date) -> Games:
        """Fetches games for a specific team within a date range."""
        url = f'https://statsapi.mlb.com/api/v1/schedule?startDate={date.start_date}&endDate={date.end_date}&sportId=1&teamId={team.value}'

        return await fetch_model_from_url(url, Games)