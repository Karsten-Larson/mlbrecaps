from mlbrecaps import Date, Games, Team, BroadcastType

from pathlib import Path
import asyncio

async def main():
    team = Team.MIN
    games = await Games.get_games_by_team(team, Date("2025-07-01", "2025-07-31"))

    plays = games.plays \
        .filter_for_events() \
        .sort_by_delta_team_win_exp(team) \
        .head(10) \
        .sort_chronologically()
                    
    output_dir = Path() / "clips"
    output_dir.mkdir(exist_ok=True)

    await plays.download_clips(output_dir, BroadcastType.HOME, verbose=True)

if __name__ == "__main__":
    asyncio.run(main())