"""
This module contains the KickEvent class, which represents a parsed Kick event.
"""

from dataclasses import dataclass
from typing import Dict, Any


@dataclass
class KickEvent:
    """
    Represents a parsed Kick event.

    Attributes:
        event (str): The event type.
        data (Dict[str, Any]): The parsed data from the event.
    """

    event: str
    data: Dict[str, Any]
