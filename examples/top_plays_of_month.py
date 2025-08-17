from mlbrecaps import Date, Games, PlayField, Team, BroadcastType

from pathlib import Path
import asyncio

async def main():
    team = Team.MIN
    games = await Games.get_games_by_team(team, Date("2025-07-01", "2025-07-31"))
    plays = games.plays \
        .filter_for_events()
        
    home_plays = plays \
        .filter(PlayField.HOME_TEAM, team) \
        .sort_by(PlayField.DELTA_HOME_WIN_EXP, ascending=False) \
        .head(10)
    
    away_plays = plays \
        .filter(PlayField.AWAY_TEAM, team) \
        .sort_by(PlayField.DELTA_HOME_WIN_EXP, ascending=True) \
        .head(10)
        
    total_plays = (home_plays + away_plays) \
        .sort_by(PlayField.DELTA_HOME_WIN_EXP, key=abs, ascending=False) \
        .head(10) \
        .sort_chronologically()
            
    output_dir = Path() / "clips"
    try:
        output_dir.mkdir(exist_ok=False)
    except FileExistsError:
        print(f"Directory {output_dir} already exists. Clips will be downloaded there. Ensure directory is empty.")
        exit(1)

    await total_plays.download_clips(output_dir, BroadcastType.HOME, True)

if __name__ == "__main__":
    asyncio.run(main())