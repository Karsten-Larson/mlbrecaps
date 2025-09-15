from pydantic import BaseModel, Field
from typing import Optional, Literal
from enum import Enum

class Play(BaseModel):
    play_id: str
    pitch_type: str
    game_date: str
    release_speed: Optional[float] = None
    release_pos_x: Optional[float] = None
    release_pos_z: Optional[float] = None
    player_name: str
    batter: int
    pitcher: int
    events: Optional[str] = None
    description: str
    spin_dir: Optional[float] = None
    zone: int
    long_description: str = Field(..., alias="des")  # alias for 'des', serves as a long description
    game_type: str
    stand: Literal['L', 'R']
    p_throws: Literal['L', 'R']
    home_team: str
    away_team: str
    type: Literal['X', 'S', 'B']
    hit_location: Optional[int] = None
    bb_type: Optional[str] = None
    balls: int
    strikes: int
    game_year: int
    pfx_x: Optional[float] = None
    pfx_z: Optional[float] = None
    plate_x: Optional[float] = None
    plate_z: Optional[float] = None
    on_3b: Optional[int] = None
    on_2b: Optional[int] = None
    on_1b: Optional[int] = None
    outs_when_up: int
    inning: int
    inning_topbot: Literal['Top', 'Bot']
    hc_x: Optional[float] = None
    hc_y: Optional[float] = None
    umpire: Optional[int] = None
    sv_id: Optional[int] = None
    sz_top: Optional[float] = None
    sz_bot: Optional[float] = None
    hit_distance_sc: Optional[int] = None
    launch_speed: Optional[float] = None
    launch_angle: Optional[int] = None
    effective_speed: Optional[float] = None
    release_spin_rate: Optional[int] = None
    release_extension: Optional[float] = None
    game_pk: int
    fielder_2: int
    fielder_3: int
    fielder_4: int
    fielder_5: int
    fielder_6: int
    fielder_7: int
    fielder_8: int
    fielder_9: int
    release_pos_y: float
    estimated_ba_using_speedangle: Optional[float] = None
    estimated_woba_using_speedangle: Optional[float] = None
    woba_value: Optional[float] = None
    woba_denom: Optional[float] = None
    babip_value: Optional[float] = None
    iso_value: Optional[float] = None
    launch_speed_angle: Optional[float] = None
    at_bat_number: int
    pitch_number: int
    pitch_name: str
    home_score: int
    away_score: int
    bat_score: int
    fld_score: int
    post_away_score: int
    post_home_score: int
    post_bat_score: int
    post_fld_score: int
    if_fielding_alignment: str
    of_fielding_alignment: str
    spin_axis: int
    delta_home_win_exp: float
    delta_run_exp: float
    bat_speed: Optional[float] = None
    swing_length: Optional[float] = None
    estimated_slg_using_speedangle: Optional[float] = None
    delta_pitcher_run_exp: float
    hyper_speed: Optional[float] = None
    home_score_diff: int
    bat_score_diff: int
    home_win_exp: float
    bat_win_exp: float
    age_pit: int
    age_bat: int
    n_thruorder_pitcher: int
    n_priorpa_thisgame_player_at_bat: Optional[int] = None
    pitcher_days_since_prev_game: Optional[int] = None
    batter_days_since_prev_game: Optional[int] = None
    pitcher_days_until_next_game: Optional[int] = None
    batter_days_until_next_game: Optional[int] = None


class PlayField(Enum):
    PLAY_ID = "play_id"
    PITCH_TYPE = "pitch_type"
    GAME_DATE = "game_date"
    RELEASE_SPEED = "release_speed"
    RELEASE_POS_X = "release_pos_x"
    RELEASE_POS_Z = "release_pos_z"
    PLAYER_NAME = "player_name"
    BATTER = "batter"
    PITCHER = "pitcher"
    EVENTS = "events"
    DESCRIPTION = "description"
    SPIN_DIR = "spin_dir"
    ZONE = "zone"
    LONG_DESCRIPTION = "long_description"
    GAME_TYPE = "game_type"
    STAND = "stand"
    P_THROWS = "p_throws"
    HOME_TEAM = "home_team"
    AWAY_TEAM = "away_team"
    TYPE = "type"
    HIT_LOCATION = "hit_location"
    BB_TYPE = "bb_type"
    BALLS = "balls"
    STRIKES = "strikes"
    GAME_YEAR = "game_year"
    PFX_X = "pfx_x"
    PFX_Z = "pfx_z"
    PLATE_X = "plate_x"
    PLATE_Z = "plate_z"
    ON_3B = "on_3b"
    ON_2B = "on_2b"
    ON_1B = "on_1b"
    OUTS_WHEN_UP = "outs_when_up"
    INNING = "inning"
    INNING_TOPBOT = "inning_topbot"
    HC_X = "hc_x"
    HC_Y = "hc_y"
    UMPIRE = "umpire"
    SV_ID = "sv_id"
    SZ_TOP = "sz_top"
    SZ_BOT = "sz_bot"
    HIT_DISTANCE_SC = "hit_distance_sc"
    LAUNCH_SPEED = "launch_speed"
    LAUNCH_ANGLE = "launch_angle"
    EFFECTIVE_SPEED = "effective_speed"
    RELEASE_SPIN_RATE = "release_spin_rate"
    RELEASE_EXTENSION = "release_extension"
    GAME_PK = "game_pk"
    FIELDER_2 = "fielder_2"
    FIELDER_3 = "fielder_3"
    FIELDER_4 = "fielder_4"
    FIELDER_5 = "fielder_5"
    FIELDER_6 = "fielder_6"
    FIELDER_7 = "fielder_7"
    FIELDER_8 = "fielder_8"
    FIELDER_9 = "fielder_9"
    RELEASE_POS_Y = "release_pos_y"
    ESTIMATED_BA_USING_SPEEDANGLE = "estimated_ba_using_speedangle"
    ESTIMATED_WOBA_USING_SPEEDANGLE = "estimated_woba_using_speedangle"
    WOBA_VALUE = "woba_value"
    WOBA_DENOM = "woba_denom"
    BABIP_VALUE = "babip_value"
    ISO_VALUE = "iso_value"
    LAUNCH_SPEED_ANGLE = "launch_speed_angle"
    AT_BAT_NUMBER = "at_bat_number"
    PITCH_NUMBER = "pitch_number"
    PITCH_NAME = "pitch_name"
    HOME_SCORE = "home_score"
    AWAY_SCORE = "away_score"
    BAT_SCORE = "bat_score"
    FLD_SCORE = "fld_score"
    POST_AWAY_SCORE = "post_away_score"
    POST_HOME_SCORE = "post_home_score"
    POST_BAT_SCORE = "post_bat_score"
    POST_FLD_SCORE = "post_fld_score"
    IF_FIELDING_ALIGNMENT = "if_fielding_alignment"
    OF_FIELDING_ALIGNMENT = "of_fielding_alignment"
    SPIN_AXIS = "spin_axis"
    DELTA_HOME_WIN_EXP = "delta_home_win_exp"
    DELTA_RUN_EXP = "delta_run_exp"
    BAT_SPEED = "bat_speed"
    SWING_LENGTH = "swing_length"
    ESTIMATED_SLG_USING_SPEEDANGLE = "estimated_slg_using_speedangle"
    DELTA_PITCHER_RUN_EXP = "delta_pitcher_run_exp"
    HYPER_SPEED = "hyper_speed"
    HOME_SCORE_DIFF = "home_score_diff"
    BAT_SCORE_DIFF = "bat_score_diff"
    HOME_WIN_EXP = "home_win_exp"
    BAT_WIN_EXP = "bat_win_exp"
    AGE_PIT = "age_pit"
    AGE_BAT = "age_bat"
    N_THRUORDER_PITCHER = "n_thruorder_pitcher"
    N_PRIORPA_THISGAME_PLAYER_AT_BAT = "n_priorpa_thisgame_player_at_bat"
    PITCHER_DAYS_SINCE_PREV_GAME = "pitcher_days_since_prev_game"
    BATTER_DAYS_SINCE_PREV_GAME = "batter_days_since_prev_game"
    PITCHER_DAYS_UNTIL_NEXT_GAME = "pitcher_days_until_next_game"
    BATTER_DAYS_UNTIL_NEXT_GAME = "batter_days_until_next_game"