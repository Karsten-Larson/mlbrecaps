from bs4 import BeautifulSoup, Tag

from curl_cffi.requests.exceptions import Timeout
from pathlib import Path

from .play import Play
from .utils import fetch_html_from_url, fetch_url
from .broadcast import BroadcastType
from .team import Team

class Clip():
    """A wrapper class for Play that allows for plays to be downloaded"""

    def __init__(self, play: Play, broadcast_type: Team | BroadcastType | None = None):
        self._play: Play = play
        self.broadcast_type: BroadcastType | None = None

        # Find the broadcast type based on the given team
        if isinstance(broadcast_type, Team):
            if self._play.home_team == broadcast_type.name:
                self.broadcast_type = BroadcastType.HOME
            else:
                self.broadcast_type = BroadcastType.AWAY
        else:
            self.broadcast_type: BroadcastType | None = broadcast_type

    @property
    def play(self) -> Play:
        return self._play
    
    async def __get_url(self, site_url: str) -> str:
        """
        Gets the url of the clip to be downloaded from the savant clip
        """
        # Get the savant site
        site = await fetch_html_from_url(site_url)

        # Find the video element of the savant clip, find the source url of the clip
        soup = BeautifulSoup(site, features="lxml")
        video_obj = soup.find("video", id="sporty")

        if not isinstance(video_obj, Tag):
            raise ValueError("Clip url is not found")

        source = video_obj.find('source')

        if not isinstance(source, Tag):
            raise ValueError("Clip url is not found")

        clip_url = source.get('src')
        
        if not isinstance(clip_url, str) or clip_url is None:
            raise ValueError("Clip url is not found or not a string")

        # Return the source url of the clip so it can be downloaded later
        return clip_url

    async def __generate(self) -> str:
        """
        Generates a savant clip based on the given at-bat information

        Row must be a pandas dataframe row.
        """

        # find the broadcast type so it's always corresponding
        # to the given batter's home team's broadcast
        if self.broadcast_type:
            broadcast_type = self.broadcast_type
        elif self._play.inning_topbot == "TOP":
            broadcast_type = BroadcastType.AWAY
        else:
            broadcast_type = BroadcastType.HOME

        # with the play id find the url for the savant clip
        site_url = f"https://baseballsavant.mlb.com/sporty-videos?playId={self._play.play_id}&videoType={broadcast_type.name}"
        clip_url = await self.__get_url(site_url)

        # if the clip is alright return it
        if clip_url != "":
            return clip_url
        
        if broadcast_type == BroadcastType.NETWORK:
            raise ValueError("Clip url is not found or not a string")

        # if the clip is screwed up then it was a national tv game
        # return the correct national tv clip url
        site_url = f"https://baseballsavant.mlb.com/sporty-videos?playId={self._play.play_id}&videoType=NETWORK"
        clip_url = await self.__get_url(site_url)

        return clip_url

    async def download(self, path: str | Path, verbose: bool = False) -> Path:
        path = Path(path)

        clip_url = await self.__generate()

        # create response object
        try:
            r = await fetch_url(clip_url)
        except Timeout:
            print(f'Timeout has been raised. Link: {clip_url}')
            raise

        # Download video
        with path.open("wb") as f:
            f.write(r.content)

        # State the video was successfully downloaded
        if verbose:
            print(f"Successfully downloaded: {path.absolute()}")

        return path