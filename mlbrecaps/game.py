from pydantic import BaseModel, ConfigDict
from typing import Optional

from .plays import Plays

class Status(BaseModel):
    model_config = ConfigDict(frozen=True)
    abstractGameState: str
    codedGameState: str
    detailedState: str
    statusCode: str
    startTimeTBD: bool
    abstractGameCode: str


class LeagueRecord(BaseModel):
    model_config = ConfigDict(frozen=True)
    wins: int
    losses: int
    pct: str


class Team(BaseModel):
    model_config = ConfigDict(frozen=True)
    id: int
    name: str
    link: str


class TeamResult(BaseModel):
    model_config = ConfigDict(frozen=True)
    leagueRecord: LeagueRecord
    score: Optional[int] = None
    team: Team
    isWinner: Optional[bool] = None
    splitSquad: Optional[bool] = None
    seriesNumber: Optional[int] = None


class Teams(BaseModel):
    model_config = ConfigDict(frozen=True)
    away: TeamResult
    home: TeamResult


class Venue(BaseModel):
    model_config = ConfigDict(frozen=True)
    id: int
    name: str
    link: str


class Content(BaseModel):
    model_config = ConfigDict(frozen=True)
    link: str


class Game(BaseModel):
    model_config = ConfigDict(frozen=True)
    gamePk: int
    gameGuid: str
    link: str
    gameType: str
    season: str
    gameDate: str
    officialDate: str
    status: Status
    teams: Teams
    venue: Venue
    content: Content
    # isTie: Optional[bool] = None
    gameNumber: int
    publicFacing: bool
    doubleHeader: str
    gamedayType: str
    tiebreaker: str
    calendarEventID: str
    seasonDisplay: str
    dayNight: str
    scheduledInnings: int
    reverseHomeAwayStatus: bool
    inningBreakLength: int
    gamesInSeries: Optional[int] = None
    seriesGameNumber: Optional[int] = None
    seriesDescription: Optional[str] = None
    recordSource: str
    ifNecessary: str
    ifNecessaryDescription: str

    @property
    def plays(self) -> Plays:
        """Returns a Plays instance for the game."""
        return Plays([self.gamePk])
    
    @property
    def is_final(self) -> bool:
        """Returns True if the game is final."""
        return self.status.codedGameState == "F"
    
    @property
    def is_regular_season(self) -> bool:
        """Returns True if the game is a regular season game."""
        return self.gameType == "R"
    
    @property
    def is_valid_game(self) -> bool:
        """Returns True if the game is a valid regular season game."""
        return self.is_final and self.is_regular_season