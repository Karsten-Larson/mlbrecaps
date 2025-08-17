from pydantic import BaseModel, ConfigDict, Json
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
    score: int
    team: Team
    isWinner: bool
    splitSquad: bool
    seriesNumber: int


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
    isTie: bool
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
    gamesInSeries: int
    seriesGameNumber: int
    seriesDescription: str
    recordSource: str
    ifNecessary: str
    ifNecessaryDescription: str

    @property
    def plays(self) -> Plays:
        """Returns a Plays instance for the game."""
        return Plays([self.gamePk])