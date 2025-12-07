import asyncio
import aiohttp
import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Any
from urllib.parse import urlencode

from ..models.calendar_get import Event, Model, Calendar, FeaturedItem, Host

log = logging.getLogger("red.luma.api")


class LumaAPIError(Exception):
    """Base exception for Luma API errors."""

    pass


class LumaAPIRateLimitError(LumaAPIError):
    """Raised when API rate limit is exceeded."""

    pass


class LumaAPINotFoundError(LumaAPIError):
    """Raised when requested resource is not found."""

    pass


class LumaAPIAuthError(LumaAPIError):
    """Raised when API authentication fails."""

    pass


class LumaAPITimeoutError(LumaAPIError):
    """Raised when API request times out."""

    pass


class LumaCacheEntry:
    """Represents a cached API response."""

    def __init__(self, data: Any, expires_at: datetime):
        self.data = data
        self.expires_at = expires_at

    def is_expired(self) -> bool:
        return datetime.now(timezone.utc) >= self.expires_at


class LumaAPIClient:
    """
    Client for interacting with the Luma public API.

    Handles authentication, rate limiting, caching, and error handling.
    """

    BASE_URL = "https://api2.luma.com"
    DEFAULT_TIMEOUT = 30
    MAX_RETRIES = 3
    RETRY_DELAY = 1.0  # seconds
    RATE_LIMIT_RETRY_DELAY = 60  # seconds
    CACHE_TTL = 300  # 5 minutes

    def __init__(self, session: Optional[aiohttp.ClientSession] = None):
        self.session = session
        self._cache: Dict[str, LumaCacheEntry] = {}
        self._last_request_time = 0
        self._rate_limit_delay = 1.0  # Default 1 second between requests

    async def __aenter__(self):
        if not self.session:
            timeout = aiohttp.ClientTimeout(total=self.DEFAULT_TIMEOUT)
            self.session = aiohttp.ClientSession(timeout=timeout)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    def _get_cache_key(self, endpoint: str, params: Dict[str, Any]) -> str:
        """Generate a cache key for the request."""
        param_str = urlencode(sorted(params.items())) if params else ""
        return f"{endpoint}?{param_str}"

    def _is_cached_entry_valid(self, cache_entry: LumaCacheEntry) -> bool:
        """Check if a cached entry is still valid."""
        return not cache_entry.is_expired()

    def _get_cached_response(self, cache_key: str) -> Optional[Any]:
        """Get response from cache if valid."""
        if cache_key in self._cache:
            entry = self._cache[cache_key]
            if self._is_cached_entry_valid(entry):
                log.debug(f"Cache hit for {cache_key}")
                return entry.data
            else:
                log.debug(f"Cache expired for {cache_key}")
                del self._cache[cache_key]
        return None

    def _cache_response(self, cache_key: str, data: Any) -> None:
        """Cache a response with TTL."""
        expires_at = datetime.now(timezone.utc) + timedelta(seconds=self.CACHE_TTL)
        self._cache[cache_key] = LumaCacheEntry(data, expires_at)
        log.debug(f"Cached response for {cache_key}")

    def _clean_expired_cache(self) -> None:
        """Remove expired entries from cache."""
        expired_keys = [key for key, entry in self._cache.items() if entry.is_expired()]
        for key in expired_keys:
            del self._cache[key]
        if expired_keys:
            log.debug(f"Cleaned {len(expired_keys)} expired cache entries")

    def _calculate_rate_limit_delay(self, response_headers: Dict[str, str]) -> None:
        """Calculate appropriate rate limit delay based on response headers."""
        # Check for rate limit headers if available
        remaining = response_headers.get("x-ratelimit-remaining")
        reset_time = response_headers.get("x-ratelimit-reset")

        if remaining and int(remaining) < 10:  # Close to limit
            if reset_time:
                reset_timestamp = float(reset_time)
                current_time = time.time()
                delay = max(reset_timestamp - current_time, self.RATE_LIMIT_RETRY_DELAY)
                self._rate_limit_delay = delay
            else:
                self._rate_limit_delay = self.RATE_LIMIT_RETRY_DELAY
        else:
            # Gradually reduce delay if we're not hitting limits
            self._rate_limit_delay = max(0.5, self._rate_limit_delay * 0.9)

    async def _rate_limit_wait(self) -> None:
        """Wait to respect rate limits."""
        current_time = time.time()
        time_since_last_request = current_time - self._last_request_time

        if time_since_last_request < self._rate_limit_delay:
            wait_time = self._rate_limit_delay - time_since_last_request
            await asyncio.sleep(wait_time)

        self._last_request_time = time.time()

    def _handle_response_status(self, status: int, response_text: str) -> None:
        """Handle HTTP response status and raise appropriate exceptions."""
        if status == 200:
            return
        elif status == 401:
            raise LumaAPIAuthError(f"Authentication failed: {response_text}")
        elif status == 403:
            raise LumaAPIAuthError(f"Access forbidden: {response_text}")
        elif status == 404:
            raise LumaAPINotFoundError(f"Resource not found: {response_text}")
        elif status == 429:
            raise LumaAPIRateLimitError(f"Rate limit exceeded: {response_text}")
        elif status >= 500:
            raise LumaAPIError(f"Server error {status}: {response_text}")
        else:
            raise LumaAPIError(f"Unexpected status {status}: {response_text}")

    async def _make_request_with_retry(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        use_cache: bool = True,
    ) -> Dict[str, Any]:
        """Make a request to the Luma API with retry logic and caching."""

        # Check cache first
        cache_key = self._get_cache_key(endpoint, params or {})
        cached_response = self._get_cached_response(cache_key)
        if cached_response is not None:
            return cached_response

        # Make request with retries
        for attempt in range(self.MAX_RETRIES):
            try:
                await self._rate_limit_wait()

                url = f"{self.BASE_URL}/{endpoint}"
                async with self.session.get(url, params=params) as response:
                    response_text = await response.text()

                    # Update rate limit delay based on response
                    self._calculate_rate_limit_delay(dict(response.headers))

                    self._handle_response_status(response.status, response_text)

                    try:
                        data = await response.json()
                    except aiohttp.ContentTypeError:
                        # Some responses might not be JSON
                        data = {"raw_response": response_text}

                    # Cache successful responses
                    if use_cache:
                        self._cache_response(cache_key, data)

                    log.debug(
                        f"Successfully fetched {endpoint} (attempt {attempt + 1})"
                    )
                    return data

            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                log.warning(f"Request attempt {attempt + 1} failed: {e}")
                if attempt < self.MAX_RETRIES - 1:
                    wait_time = self.RETRY_DELAY * (2**attempt)  # Exponential backoff
                    await asyncio.sleep(wait_time)
                else:
                    raise LumaAPITimeoutError(
                        f"Request failed after {self.MAX_RETRIES} attempts: {e}"
                    )

            except LumaAPIRateLimitError as e:
                log.warning(f"Rate limit hit on attempt {attempt + 1}: {e}")
                if attempt < self.MAX_RETRIES - 1:
                    await asyncio.sleep(self.RATE_LIMIT_RETRY_DELAY)
                else:
                    raise

        # Clean up expired cache entries
        self._clean_expired_cache()

        raise LumaAPIError(f"Request failed after {self.MAX_RETRIES} attempts")

    async def get_calendar_events(
        self, calendar_identifier: str, limit: int = 50, is_slug: bool = False
    ) -> List[Event]:
        """
        Fetch events from a Luma calendar.

        Args:
            calendar_identifier: The API ID or slug of the calendar
            limit: Maximum number of events to fetch
            is_slug: True if identifier is a slug, False if it's an API ID

        Returns:
            List of Event objects
        """
        try:
            calendar_data = None
            calendar_api_id = calendar_identifier

            if is_slug:
                # First get calendar info using slug, then use the API ID
                calendar_info = await self.get_calendar_info(calendar_identifier)
                if not calendar_info:
                    raise LumaAPINotFoundError(
                        f"Calendar with slug '{calendar_identifier}' not found"
                    )

                calendar_api_id = calendar_info.get("api_id")
                if not calendar_api_id:
                    raise LumaAPIError(
                        f"Could not get API ID for calendar slug '{calendar_identifier}'"
                    )

                # Convert calendar info to Calendar object for access to slug
                try:
                    calendar_data = Calendar(**calendar_info)
                except Exception as e:
                    log.warning(f"Failed to create Calendar object: {e}")

                log.info(
                    f"Resolved calendar slug '{calendar_identifier}' to API ID '{calendar_api_id}'"
                )
            else:
                calendar_api_id = calendar_identifier

            # Use the correct API endpoint for getting calendar data
            params = {"api_id": calendar_api_id, "limit": min(limit, 100)}  # API limit

            endpoint = "calendar/get"
            response_data = await self._make_request_with_retry(endpoint, params)

            # Parse the response into Event objects
            events = []

            # Check if we have a Model response (from the existing Pydantic models)
            if isinstance(response_data, dict) and "calendar" in response_data:
                try:
                    model = Model(**response_data)

                    # Store the calendar data from the API response if we don't have it
                    if calendar_data is None:
                        try:
                            calendar_data = Calendar(**response_data["calendar"])
                        except Exception as e:
                            log.warning(
                                f"Failed to create Calendar object from response: {e}"
                            )

                    # Extract events from featured_items with their hosts and calendar info
                    for featured_item in model.featured_items:
                        if hasattr(featured_item, "event") and featured_item.event:
                            # Ensure we have calendar data, fallback to the calendar_data from API response
                            event_calendar = getattr(
                                featured_item, "calendar", calendar_data
                            )
                            if not event_calendar and calendar_data:
                                event_calendar = calendar_data

                            # Create a wrapper that includes hosts, calendar, and tags information
                            event_with_hosts = type(
                                "EventWithHosts",
                                (),
                                {
                                    "event": featured_item.event,
                                    "hosts": getattr(featured_item, "hosts", []),
                                    "calendar": event_calendar,
                                    "tags": getattr(featured_item, "tags", []),
                                    "__dict__": featured_item.event.__dict__,
                                    "__getattr__": lambda self, name: getattr(
                                        self.event, name
                                    ),
                                },
                            )()
                            events.append(event_with_hosts)

                except Exception as e:
                    log.error(f"Failed to parse Model response: {e}")
                    # Fall back to raw data parsing

            # Fallback: Try to parse raw response data
            if not events and isinstance(response_data, dict):
                # Look for events in various possible locations
                events_data = []
                hosts_data = {}
                calendar_info_data = response_data.get("calendar")

                # Try featured_items first
                if "featured_items" in response_data:
                    for item in response_data["featured_items"]:
                        if "event" in item:
                            events_data.append(item["event"])
                            # Store associated hosts if available
                            if "hosts" in item:
                                hosts_data[item["event"].get("api_id", "unknown")] = (
                                    item["hosts"]
                                )

                # Try events directly
                if "events" in response_data:
                    events_data.extend(response_data["events"])

                # Parse events into Event objects
                for event_data in events_data:
                    try:
                        if isinstance(event_data, dict):
                            event = Event(**event_data)
                            # Wrap with hosts if available
                            event_id = event_data.get("api_id", "unknown")
                            if event_id in hosts_data:
                                # Ensure we have calendar data, use the best available source
                                event_calendar = calendar_data
                                if not event_calendar and calendar_info_data:
                                    try:
                                        event_calendar = Calendar(**calendar_info_data)
                                    except Exception as e:
                                        log.warning(
                                            f"Failed to create Calendar object from calendar_info_data: {e}"
                                        )
                                        event_calendar = None

                                # Get tags for this event from the original response
                                event_tags = []
                                for item in response_data.get("featured_items", []):
                                    if (
                                        item.get("event", {}).get("api_id") == event_id
                                        and "tags" in item
                                    ):
                                        event_tags = item["tags"]
                                        break

                                event_with_hosts = type(
                                    "EventWithHosts",
                                    (),
                                    {
                                        "event": event,
                                        "hosts": hosts_data[event_id],
                                        "calendar": event_calendar,
                                        "tags": event_tags,
                                        "__dict__": event.__dict__,
                                        "__getattr__": lambda self, name: getattr(
                                            self.event, name
                                        ),
                                    },
                                )()
                                events.append(event_with_hosts)
                            else:
                                # Even without hosts, wrap with calendar data if available
                                if calendar_data:
                                    # Get tags for this event from the original response
                                    event_tags = []
                                    for item in response_data.get("featured_items", []):
                                        if (
                                            item.get("event", {}).get("api_id")
                                            == event_data.get("api_id")
                                            and "tags" in item
                                        ):
                                            event_tags = item["tags"]
                                            break

                                    event_with_calendar = type(
                                        "EventWithCalendar",
                                        (),
                                        {
                                            "event": event,
                                            "hosts": [],
                                            "calendar": calendar_data,
                                            "tags": event_tags,
                                            "__dict__": event.__dict__,
                                            "__getattr__": lambda self, name: getattr(
                                                self.event, name
                                            ),
                                        },
                                    )()
                                    events.append(event_with_calendar)
                                else:
                                    events.append(event)
                        elif hasattr(event_data, "event"):  # If it's a featured_item
                            events.append(event_data.event)
                    except Exception as e:
                        log.warning(f"Failed to parse event data: {e}")
                        continue

            log.info(
                f"Successfully fetched {len(events)} events from calendar {calendar_api_id}"
            )
            return events[:limit]  # Ensure we don't exceed the requested limit

        except Exception as e:
            log.error(f"Failed to fetch events for calendar {calendar_identifier}: {e}")
            raise LumaAPIError(f"Failed to fetch events: {e}")

    async def get_calendar_info(self, calendar_slug: str) -> Optional[Dict[str, Any]]:
        """
        Get basic information about a calendar.

        Args:
            calendar_slug: The slug/URL identifier of the calendar

        Returns:
            Dictionary with calendar information or None if not found
        """
        try:
            endpoint = f"v1/public/calendars/{calendar_slug}"
            response_data = await self._make_request_with_retry(endpoint)

            if isinstance(response_data, dict) and "calendar" in response_data:
                return response_data["calendar"]
            elif isinstance(response_data, dict):
                return response_data

            return None

        except LumaAPINotFoundError:
            log.warning(f"Calendar {calendar_slug} not found")
            return None
        except Exception as e:
            log.error(f"Failed to get calendar info for {calendar_slug}: {e}")
            return None

    def clear_cache(self) -> None:
        """Clear all cached responses."""
        self._cache.clear()
        log.info("API cache cleared")

    def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        total_entries = len(self._cache)
        expired_entries = sum(1 for entry in self._cache.values() if entry.is_expired())

        return {
            "total_entries": total_entries,
            "valid_entries": total_entries - expired_entries,
            "expired_entries": expired_entries,
        }
