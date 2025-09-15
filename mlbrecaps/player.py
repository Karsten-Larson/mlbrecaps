from __future__ import annotations

from pydantic import BaseModel

from .utils import fetch_models_from_url

class Player(BaseModel):
    name: str
    id: int
    is_player: int
    mlb: int
    league: str
    first: str
    is_prospect: int
    parent_team: str | None = None
    pos: str
    rank: int | None = None
    last_year: str
    name_display_club: str

    @staticmethod
    async def from_fullname(full_name: str) -> list[Player]:
        search_name = full_name.strip().replace(" ", "%20")
        url = f"https://baseballsavant.mlb.com/player/search-all?search={search_name}"
        return await fetch_models_from_url(url, Player)
