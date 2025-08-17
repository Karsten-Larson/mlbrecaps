from enum import Enum, auto

class BroadcastType(Enum):
    """
    Enum representing different types of broadcasts.
    """
    HOME = auto()
    AWAY = auto()
    NETWORK = auto()