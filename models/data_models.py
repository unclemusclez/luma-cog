from typing import List, Dict, Any, Optional
from dataclasses import dataclass, asdict


@dataclass
class Subscription:
    """Represents a Luma calendar subscription."""

    api_id: str
    slug: str
    name: str
    added_by: int
    added_at: str

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Subscription":
        return cls(**data)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class ChannelGroup:
    """Represents a channel group for displaying events."""

    name: str
    channel_id: int
    subscription_ids: List[str]
    max_events: int
    created_by: int
    created_at: str

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ChannelGroup":
        return cls(**data)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class LumaConfig:
    """Configuration for the Luma cog."""

    subscriptions: Dict[str, Dict[str, Any]]
    channel_groups: Dict[str, Dict[str, Any]]
    update_interval_hours: int
    last_update: Optional[str]
    enabled: bool

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "LumaConfig":
        return cls(**data)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
