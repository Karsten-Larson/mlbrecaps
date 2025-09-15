from mlbrecaps import Season, Games, Team, BroadcastType, Player

from pathlib import Path
import asyncio

async def main():
    team = Team.MIN
    player = (await Player.from_fullname("Byron Buxton"))[0]
    games = await Games.get_games_by_team(team, Season(2025))

    # Get the top 10 plays of the season for Byron Buxton, order from worst to best
    plays = games.plays \
        .filter_for_batter(player) \
        .filter_for_events() \
        .sort_by_delta_team_win_exp(team) \
        .head(10) \
        .reverse() # switch ordering from worst to best
                            
    output_dir = Path() / "clips"
    output_dir.mkdir(exist_ok=True)

    await plays.download_clips(output_dir, BroadcastType.HOME, True)

if __name__ == "__main__":
    asyncio.run(main())