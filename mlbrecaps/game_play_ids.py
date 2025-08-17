from pydantic import BaseModel


class Scoreboard(BaseModel):
    gamePk: int


class PlayItem(BaseModel):
    play_id: str
    inning: int
    ab_number: int
    cap_index: int
    outs: int
    batter: int
    pitcher: int
    pitch_number: int
    player_total_pitches: int
    game_total_pitches: int
    rowId: str
    game_pk: int

class GamePlayIds(BaseModel):
    game_status_code: str
    game_status: str
    gamedayType: str
    gameDate: str
    scoreboard: Scoreboard
    team_home: list[PlayItem]
    team_away: list[PlayItem]

    @property
    def play_data(self) -> list[PlayItem]:
        return self.team_home + self.team_away
