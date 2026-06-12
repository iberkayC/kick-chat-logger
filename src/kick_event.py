"""KickEvent, the dataclass representing a parsed Kick event."""

from dataclasses import dataclass
from typing import Any


@dataclass
class KickEvent:
    """Represents a parsed Kick event.

    Attributes:
        event (str): The event type.
        data (Dict[str, Any]): The parsed data from the event.

    """

    event: str
    data: dict[str, Any]
