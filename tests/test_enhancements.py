"""
Test suite for Luma RedBot cog enhancements.

This test file verifies that all enhancements maintain backward compatibility
while adding new functionality.
"""

import pytest
import asyncio
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

from luma.models.data_models import Subscription, ChannelGroup
from luma.core.database import EventDatabase
from luma.models.calendar_get import Event, VirtualInfo, GeoAddressInfo, Coordinate1


class TestBackwardCompatibility:
    """Test backward compatibility of enhancements."""

    def test_channel_group_without_timezone(self):
        """Test that existing channel groups without timezone still work."""
        # Simulate old channel group data (without timezone field)
        old_group_data = {
            "name": "test_group",
            "channel_id": 12345,
            "subscription_ids": ["sub1", "sub2"],
            "max_events": 10,
            "created_by": 67890,
            "created_at": "2023-01-01T00:00:00Z",
        }

        # Should work without timezone
        group = ChannelGroup.from_dict(old_group_data)
        assert group.name == "test_group"
        assert group.timezone is None
        assert group.subscription_ids == ["sub1", "sub2"]

    def test_channel_group_with_timezone(self):
        """Test that new channel groups with timezone work."""
        new_group_data = {
            "name": "test_group",
            "channel_id": 12345,
            "subscription_ids": ["sub1", "sub2"],
            "max_events": 10,
            "created_by": 67890,
            "created_at": "2023-01-01T00:00:00Z",
            "timezone": "America/New_York",
        }

        group = ChannelGroup.from_dict(new_group_data)
        assert group.name == "test_group"
        assert group.timezone == "America/New_York"
        assert group.subscription_ids == ["sub1", "sub2"]


class TestDatabaseEnhancements:
    """Test database event tracking functionality."""

    @pytest.fixture
    def event_db(self):
        """Create temporary database for testing."""
        return EventDatabase(":memory:")

    @pytest.mark.asyncio
    async def test_upsert_events(self, event_db):
        """Test event upsert functionality."""
        # Test events
        test_events = [
            {
                "api_id": "event1",
                "calendar_api_id": "cal1",
                "name": "Test Event 1",
                "start_at": "2023-12-01T10:00:00Z",
                "timezone": "UTC",
                "event_type": "meeting",
                "url": "https://example.com/event1",
            },
            {
                "api_id": "event2",
                "calendar_api_id": "cal1",
                "name": "Test Event 2",
                "start_at": "2023-12-02T11:00:00Z",
                "timezone": "UTC",
                "event_type": "workshop",
                "url": "https://example.com/event2",
            },
        ]

        # Insert events
        result = await event_db.upsert_events(test_events, "cal1")
        assert result["new_events"] == 2
        assert result["updated_events"] == 0
        assert result["deleted_events"] == 0

        # Update one event
        test_events[0]["name"] = "Updated Test Event 1"
        result = await event_db.upsert_events(test_events, "cal1")
        assert result["new_events"] == 0
        assert result["updated_events"] == 1
        assert result["deleted_events"] == 0

    @pytest.mark.asyncio
    async def test_get_new_events(self, event_db):
        """Test getting only new events."""
        # Add initial events
        initial_events = [
            {
                "api_id": "event1",
                "calendar_api_id": "cal1",
                "name": "Test Event 1",
                "start_at": "2023-12-01T10:00:00Z",
                "timezone": "UTC",
                "event_type": "meeting",
                "url": "https://example.com/event1",
            }
        ]
        await event_db.upsert_events(initial_events, "cal1")

        # Add new event
        new_events = [
            {
                "api_id": "event1",
                "calendar_api_id": "cal1",
                "name": "Test Event 1",
                "start_at": "2023-12-01T10:00:00Z",
                "timezone": "UTC",
                "event_type": "meeting",
                "url": "https://example.com/event1",
            },
            {
                "api_id": "event2",
                "calendar_api_id": "cal1",
                "name": "Test Event 2",
                "start_at": "2023-12-02T11:00:00Z",
                "timezone": "UTC",
                "event_type": "workshop",
                "url": "https://example.com/event2",
            },
        ]

        only_new = await event_db.get_new_events("cal1", new_events)
        assert len(only_new) == 1
        assert only_new[0]["api_id"] == "event2"


class TestAPIEnhancements:
    """Test API client enhancements."""

    @pytest.mark.asyncio
    async def test_get_calendar_events_with_slug(self):
        """Test that API client can handle both slug and API ID."""
        # Mock the API client
        with patch("luma.core.api_client.LumaAPIClient") as mock_client:
            mock_instance = AsyncMock()
            mock_client.return_value.__aenter__.return_value = mock_instance

            # Mock get_calendar_info response
            mock_instance.get_calendar_info.return_value = {
                "api_id": "resolved_api_id_123"
            }

            # Mock get_calendar_events response
            mock_instance._make_request_with_retry.return_value = {
                "calendar": {"name": "Test Calendar"},
                "featured_items": [],
            }

            client = LumaAPIClient()

            # Test with slug
            result = await client.get_calendar_events(
                calendar_identifier="test-calendar-slug", is_slug=True
            )

            # Verify that get_calendar_info was called
            mock_instance.get_calendar_info.assert_called_once_with(
                "test-calendar-slug"
            )

    @pytest.mark.asyncio
    async def test_get_calendar_events_with_api_id(self):
        """Test that API client still works with API ID."""
        with patch("luma.core.api_client.LumaAPIClient") as mock_client:
            mock_instance = AsyncMock()
            mock_client.return_value.__aenter__.return_value = mock_instance

            # Mock response
            mock_instance._make_request_with_retry.return_value = {
                "calendar": {"name": "Test Calendar"},
                "featured_items": [],
            }

            client = LumaAPIClient()

            # Test with API ID
            result = await client.get_calendar_events(
                calendar_identifier="api_id_123", is_slug=False
            )

            # Verify that get_calendar_info was NOT called
            mock_instance.get_calendar_info.assert_not_called()


class TestEventFormatting:
    """Test improved event formatting."""

    def test_event_with_all_fields(self):
        """Test event object with all enhanced fields."""
        event = Event(
            api_id="test_event_1",
            calendar_api_id="test_calendar",
            cover_url="https://example.com/cover.jpg",
            end_at="2023-12-01T12:00:00Z",
            event_type="workshop",  # Enhanced field
            hide_rsvp=False,
            location_type="physical",
            name="Test Workshop Event",
            one_to_one=False,
            recurrence_id=None,
            show_guest_list=True,
            start_at="2023-12-01T10:00:00Z",
            timezone="America/New_York",  # Enhanced field
            url="https://luma.com/test-event",
            user_api_id="user123",
            visibility="public",
            waitlist_enabled=False,
            virtual_info=VirtualInfo(has_access=True),
            geo_address_info=GeoAddressInfo(mode="physical", city_state="New York, NY"),
            geo_address_visibility="public",
            coordinate=Coordinate1(latitude=40.7128, longitude=-74.0060),
        )

        # Verify enhanced fields
        assert event.event_type == "workshop"
        assert event.timezone == "America/New_York"
        assert event.url == "https://luma.com/test-event"


class TestConfigurationEnhancements:
    """Test new configuration options."""

    def test_create_group_with_timezone(self):
        """Test creating channel group with timezone."""
        group_data = {
            "name": "timezone_group",
            "channel_id": 12345,
            "subscription_ids": [],
            "max_events": 15,
            "created_by": 67890,
            "created_at": "2023-01-01T00:00:00Z",
            "timezone": "Europe/London",
        }

        group = ChannelGroup.from_dict(group_data)
        assert group.timezone == "Europe/London"
        assert group.max_events == 15

    def test_subscription_data_models(self):
        """Test that subscription models remain compatible."""
        subscription_data = {
            "api_id": "api123",
            "slug": "test-calendar",
            "name": "Test Calendar",
            "added_by": 12345,
            "added_at": "2023-01-01T00:00:00Z",
        }

        subscription = Subscription.from_dict(subscription_data)
        assert subscription.api_id == "api123"
        assert subscription.slug == "test-calendar"
        assert subscription.name == "Test Calendar"


if __name__ == "__main__":
    # Run tests
    pytest.main([__file__, "-v"])
