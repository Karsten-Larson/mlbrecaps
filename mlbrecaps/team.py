from enum import Enum

class Team(Enum):
    """
    Enum representing MLB teams.
    """
    LAA=108
    AZ=109
    BAL=110
    BOS=111
    CHC=112
    CIN=113
    CLE=114
    COL=115
    DET=116
    HOU=117
    KC=118
    LAD=119
    WSH=120
    NYM=121
    OAK=133
    ATH=133.1 # Allows for both OAK and ATH to be used interchangeably without aliasing
    PIT=134
    SD=135
    SEA=136
    SF=137
    STL=138
    TB=139
    TEX=140
    TOR=141
    MIN=142
    PHI=143
    ATL=144
    CWS=145
    MIA=146
    NYY=147
    MIL=158
    AL=159
    NL=160

    @property
    def value(self) -> int:
        """
        Returns the integer value of the team.
        """
        return int(super().value)