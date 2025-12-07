import sqlite3
import logging
import asyncio
from datetime import datetime, timezone
from typing import List, Dict, Optional, Any
from pathlib import Path

log = logging.getLogger("red.luma.database")


class EventDatabase:
    """
    SQLite database for tracking Luma events to detect changes and avoid duplicates.

    Database schema:
    - events: Store event metadata for change detection
    - event_history: Track when events were sent to Discord
    """

    def __init__(self, db_path: str = "luma_events.db"):
        self.db_path = db_path
        self._lock = asyncio.Lock()
        self._initialize_database()

    def _initialize_database(self):
        """Initialize the database and create tables if they don't exist."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()

                # Create events table for tracking unique events
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS events (
                        event_api_id TEXT PRIMARY KEY,
                        calendar_api_id TEXT NOT NULL,
                        name TEXT NOT NULL,
                        start_at TEXT NOT NULL,
                        end_at TEXT,
                        timezone TEXT,
                        event_type TEXT,
                        url TEXT,
                        last_modified TEXT,
                        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                        updated_at TEXT DEFAULT CURRENT_TIMESTAMP
                    )
                """
                )

                # Create event_history table for tracking sent messages
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS event_history (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        event_api_id TEXT NOT NULL,
                        guild_id INTEGER NOT NULL,
                        channel_id INTEGER NOT NULL,
                        sent_at TEXT DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (event_api_id) REFERENCES events (event_api_id)
                    )
                """
                )

                # Create indexes for performance
                cursor.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_events_calendar 
                    ON events(calendar_api_id)
                """
                )

                cursor.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_events_start_time 
                    ON events(start_at)
                """
                )

                cursor.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_event_history_guild 
                    ON event_history(guild_id, channel_id)
                """
                )

                conn.commit()
                log.info("Event database initialized successfully")

        except Exception as e:
            log.error(f"Failed to initialize database: {e}")
            raise

    async def get_tracked_events(self, calendar_api_id: str) -> List[Dict[str, Any]]:
        """Get all tracked events for a specific calendar."""
        async with self._lock:
            try:
                with sqlite3.connect(self.db_path) as conn:
                    conn.row_factory = sqlite3.Row
                    cursor = conn.cursor()

                    cursor.execute(
                        """
                        SELECT * FROM events 
                        WHERE calendar_api_id = ? 
                        ORDER BY start_at
                    """,
                        (calendar_api_id,),
                    )

                    return [dict(row) for row in cursor.fetchall()]

            except Exception as e:
                log.error(f"Failed to get tracked events for {calendar_api_id}: {e}")
                return []

    async def upsert_events(
        self, events: List[Dict[str, Any]], calendar_api_id: str
    ) -> Dict[str, Any]:
        """
        Update or insert events and return information about changes.

        Returns:
            Dict with 'new_events', 'updated_events', 'deleted_events' counts
        """
        async with self._lock:
            try:
                with sqlite3.connect(self.db_path) as conn:
                    cursor = conn.cursor()

                    # Get existing events for this calendar
                    cursor.execute(
                        """
                        SELECT event_api_id, last_modified FROM events 
                        WHERE calendar_api_id = ?
                    """,
                        (calendar_api_id,),
                    )
                    existing_events = {row[0]: row[1] for row in cursor.fetchall()}

                    new_events = 0
                    updated_events = 0
                    current_time = datetime.now(timezone.utc).isoformat()

                    # Process incoming events
                    for event_data in events:
                        event_api_id = event_data["api_id"]

                        if event_api_id in existing_events:
                            # Check if event was modified
                            if existing_events[event_api_id] != event_data.get(
                                "last_modified", current_time
                            ):
                                # Update existing event
                                cursor.execute(
                                    """
                                    UPDATE events SET
                                        name = ?, start_at = ?, end_at = ?, timezone = ?,
                                        event_type = ?, url = ?, last_modified = ?,
                                        updated_at = ?
                                    WHERE event_api_id = ?
                                """,
                                    (
                                        event_data.get("name", ""),
                                        event_data.get("start_at", ""),
                                        event_data.get("end_at"),
                                        event_data.get("timezone"),
                                        event_data.get("event_type"),
                                        event_data.get("url"),
                                        event_data.get("last_modified", current_time),
                                        current_time,
                                        event_api_id,
                                    ),
                                )
                                updated_events += 1
                        else:
                            # Insert new event
                            cursor.execute(
                                """
                                INSERT INTO events (
                                    event_api_id, calendar_api_id, name, start_at, end_at,
                                    timezone, event_type, url, last_modified, created_at, updated_at
                                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """,
                                (
                                    event_api_id,
                                    calendar_api_id,
                                    event_data.get("name", ""),
                                    event_data.get("start_at", ""),
                                    event_data.get("end_at"),
                                    event_data.get("timezone"),
                                    event_data.get("event_type"),
                                    event_data.get("url"),
                                    event_data.get("last_modified", current_time),
                                    current_time,
                                    current_time,
                                ),
                            )
                            new_events += 1

                    # Find deleted events (events that existed before but not in current data)
                    current_event_ids = {event["api_id"] for event in events}
                    deleted_events = len(existing_events) - len(
                        current_event_ids.intersection(existing_events.keys())
                    )

                    # Clean up deleted events
                    if deleted_events > 0:
                        deleted_event_ids = list(
                            set(existing_events.keys()) - current_event_ids
                        )
                        placeholders = ",".join(["?" for _ in deleted_event_ids])
                        cursor.execute(
                            f"""
                            DELETE FROM events 
                            WHERE event_api_id IN ({placeholders})
                        """,
                            deleted_event_ids,
                        )

                    conn.commit()

                    log.info(
                        f"Event sync for {calendar_api_id}: {new_events} new, {updated_events} updated, {deleted_events} deleted"
                    )

                    return {
                        "new_events": new_events,
                        "updated_events": updated_events,
                        "deleted_events": deleted_events,
                        "total_events": len(events),
                    }

            except Exception as e:
                log.error(f"Failed to upsert events for {calendar_api_id}: {e}")
                return {
                    "new_events": 0,
                    "updated_events": 0,
                    "deleted_events": 0,
                    "total_events": 0,
                }

    async def get_new_events(
        self, calendar_api_id: str, events: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Get only the new events that haven't been tracked before."""
        async with self._lock:
            try:
                with sqlite3.connect(self.db_path) as conn:
                    cursor = conn.cursor()

                    cursor.execute(
                        """
                        SELECT event_api_id FROM events 
                        WHERE calendar_api_id = ?
                    """,
                        (calendar_api_id,),
                    )

                    existing_event_ids = {row[0] for row in cursor.fetchall()}

                    new_events = [
                        event
                        for event in events
                        if event["api_id"] not in existing_event_ids
                    ]

                    return new_events

            except Exception as e:
                log.error(f"Failed to get new events for {calendar_api_id}: {e}")
                return events  # Return all events if database query fails

    async def record_event_sent(
        self, event_api_id: str, guild_id: int, channel_id: int
    ):
        """Record that an event was sent to a Discord channel."""
        async with self._lock:
            try:
                with sqlite3.connect(self.db_path) as conn:
                    cursor = conn.cursor()

                    cursor.execute(
                        """
                        INSERT INTO event_history (event_api_id, guild_id, channel_id)
                        VALUES (?, ?, ?)
                    """,
                        (event_api_id, guild_id, channel_id),
                    )

                    conn.commit()
                    log.debug(
                        f"Recorded event {event_api_id} sent to guild {guild_id}, channel {channel_id}"
                    )

            except Exception as e:
                log.error(f"Failed to record event sent: {e}")

    async def was_event_recently_sent(
        self, event_api_id: str, guild_id: int, hours: int = 24
    ) -> bool:
        """Check if an event was sent to this guild within the specified hours."""
        async with self._lock:
            try:
                with sqlite3.connect(self.db_path) as conn:
                    cursor = conn.cursor()

                    cutoff_time = datetime.now(timezone.utc).isoformat()
                    hours_ago = (
                        datetime.now(timezone.utc)
                        .replace(hour=datetime.now(timezone.utc).hour - hours)
                        .isoformat()
                    )

                    cursor.execute(
                        """
                        SELECT COUNT(*) FROM event_history 
                        WHERE event_api_id = ? AND guild_id = ? AND sent_at >= ?
                    """,
                        (event_api_id, guild_id, hours_ago),
                    )

                    count = cursor.fetchone()[0]
                    return count > 0

            except Exception as e:
                log.error(f"Failed to check recent event send: {e}")
                return False

    async def get_calendar_stats(self) -> Dict[str, Any]:
        """Get statistics about tracked calendars and events."""
        async with self._lock:
            try:
                with sqlite3.connect(self.db_path) as conn:
                    cursor = conn.cursor()

                    # Get total counts
                    cursor.execute("SELECT COUNT(*) FROM events")
                    total_events = cursor.fetchone()[0]

                    cursor.execute("SELECT COUNT(DISTINCT calendar_api_id) FROM events")
                    total_calendars = cursor.fetchone()[0]

                    cursor.execute("SELECT COUNT(*) FROM event_history")
                    total_sends = cursor.fetchone()[0]

                    # Get calendar-specific stats
                    cursor.execute(
                        """
                        SELECT calendar_api_id, COUNT(*) as event_count 
                        FROM events 
                        GROUP BY calendar_api_id 
                        ORDER BY event_count DESC
                    """
                    )

                    calendar_stats = [
                        {"calendar_api_id": row[0], "event_count": row[1]}
                        for row in cursor.fetchall()
                    ]

                    return {
                        "total_events": total_events,
                        "total_calendars": total_calendars,
                        "total_sends": total_sends,
                        "calendar_stats": calendar_stats,
                    }

            except Exception as e:
                log.error(f"Failed to get calendar stats: {e}")
                return {}

    async def cleanup_old_history(self, days: int = 30):
        """Clean up old event history records to prevent database bloat."""
        async with self._lock:
            try:
                with sqlite3.connect(self.db_path) as conn:
                    cursor = conn.cursor()

                    cutoff_date = (
                        datetime.now(timezone.utc)
                        .replace(day=datetime.now(timezone.utc).day - days)
                        .isoformat()
                    )

                    cursor.execute(
                        """
                        DELETE FROM event_history 
                        WHERE sent_at < ?
                    """,
                        (cutoff_date,),
                    )

                    deleted_count = cursor.rowcount
                    conn.commit()

                    log.info(f"Cleaned up {deleted_count} old event history records")
                    return deleted_count

            except Exception as e:
                log.error(f"Failed to cleanup old history: {e}")
                return 0
