import sys
import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Any, Optional
import discord
from redbot.core import Config, commands, checks
from redbot.core.bot import Red
from redbot.core.utils import menus

from ..models.calendar_get import Event
from ..models.data_models import Subscription, ChannelGroup
from .api_client import (
    LumaAPIClient,
    LumaAPIError,
    LumaAPIRateLimitError,
    LumaAPINotFoundError,
)
from .database import EventDatabase


log = logging.getLogger("red.luma")

# End-User Data Statement for Redbot compliance
__data_statement__ = """
This cog stores the following data for each Discord server (guild) that uses it:

1. Luma API credentials and calendar subscription information
   - Luma API IDs, slugs, and friendly names for subscribed calendars
   - Configuration for channel groups and update intervals
   - Timestamps of when subscriptions were added

2. Data is stored using Red's Config system with the following keys:
   Global settings:
   - update_interval_hours: Integer for update frequency (global for all guilds)
   - last_update: ISO timestamp of last update
   
   Per-guild settings:
   - subscriptions: Dict of API ID -> subscription details
   - channel_groups: Dict of group name -> channel group configuration
   - enabled: Boolean for automatic updates (per guild)

3. No personal user data is collected or stored
4. All data is associated with Discord server IDs
5. Data can be deleted using [p]luma reset command or by removing the cog

This data is necessary for the cog to function as intended - displaying Luma calendar events in Discord channels.
Users have the right to delete this data at any time using the available commands or by removing the cog from their server.
"""


class Luma(commands.Cog):
    """
    Luma Events Plugin for Red-DiscordBot

    A comprehensive cog for managing Luma calendar subscriptions and displaying events across Discord channels.

    Features:
    - Add multiple Luma calendar subscriptions per server
    - Create channel groups to organize event displays
    - Automatic background updates with configurable intervals
    - Manual testing and forced updates
    - Rich embeds with event details and links

    Requires administrator permissions to configure.

    Example usage:
    [p]luma subscriptions add calendar_api_id calendar_slug "My Events"
    [p]luma groups create "Weekly Events" #general 15
    [p]luma groups addsub "Weekly Events" calendar_api_id
    [p]luma config interval 6
    """

    def __init__(self, bot: Red):
        self.bot = bot
        self.config = Config.get_conf(
            self, identifier=928374927364, force_registration=True
        )

        # Initialize event database
        self.event_db = EventDatabase(cog_instance=self)

        # Default configuration - global settings
        self.config.register_global(update_interval_hours=24, last_update=None)

        # Default guild configuration
        default_guild = {
            "subscriptions": {},
            "channel_groups": {},
            "enabled": True,
        }

        self.config.register_guild(**default_guild)

        # Background task for updating events
        self.update_task = None
        self.bot.loop.create_task(self.initialize())

    async def initialize(self):
        """Initialize the cog and start background tasks.

        This method is called when the cog is loaded. It waits for the bot
        to be ready and then starts the background update task.
        For multi-guild support, each guild manages its own enabled status.
        """
        await self.bot.wait_until_ready()
        await self.start_update_task()

    def cog_unload(self):
        """Clean up when cog is unloaded."""
        if self.update_task:
            self.update_task.cancel()

    async def start_update_task(self):
        """Start the background task for updating events."""
        if self.update_task and not self.update_task.done():
            self.update_task.cancel()

        self.update_task = self.bot.loop.create_task(self.update_events_loop())

    async def update_events_loop(self):
        """Main loop for updating events from all subscriptions."""
        while True:
            try:
                await self.update_all_events()
                update_interval = await self.config.update_interval_hours()
                await asyncio.sleep(update_interval * 3600)  # Convert hours to seconds
            except Exception as e:
                log.error(f"Error in update events loop: {e}")
                await asyncio.sleep(300)  # Wait 5 minutes before retrying

    async def update_all_events(self):
        """Update events for all guilds and their subscriptions."""
        for guild in self.bot.guilds:
            try:
                # Check if this guild has enabled updates
                if await self.config.guild(guild).enabled():
                    await self.update_guild_events(guild)
                else:
                    log.debug(f"Updates disabled for guild {guild.id}, skipping")
            except Exception as e:
                log.error(f"Error updating events for guild {guild.id}: {e}")

        await self.config.last_update.set(datetime.now(timezone.utc).isoformat())

    async def update_guild_events(self, guild: discord.Guild):
        """Update events for a specific guild."""
        subscriptions = await self.config.guild(guild).subscriptions()
        channel_groups = await self.config.guild(guild).channel_groups()

        for group_name, group_data in channel_groups.items():
            group = ChannelGroup.from_dict(group_data)
            result = await self.fetch_events_for_group(
                group, subscriptions, check_for_changes=True
            )

            # Only send message if there are new events
            if result["events"] and result["new_events_count"] > 0:
                log.info(
                    f"Sending {len(result['events'])} new events to group '{group_name}' "
                    f"(detected {result['new_events_count']} new events)"
                )
                await self.send_events_to_channel(
                    group.channel_id, result["events"], guild, group_name
                )
            else:
                log.debug(f"No new events for group '{group_name}', skipping update")

    async def fetch_events_for_group(
        self, group: ChannelGroup, subscriptions: Dict, check_for_changes: bool = True
    ) -> Dict[str, Any]:
        """Fetch events for a specific channel group with change detection."""
        all_events = []
        total_new_events = 0
        change_stats = {"new_events": 0, "updated_events": 0, "deleted_events": 0}

        for sub_id in group.subscription_ids:
            if sub_id in subscriptions:
                subscription = Subscription.from_dict(subscriptions[sub_id])
                try:
                    result = await self.fetch_events_from_subscription(
                        subscription, check_for_changes
                    )
                    all_events.extend(result["events"])
                    total_new_events += len(result["new_events"])

                    # Aggregate change stats
                    for key in change_stats:
                        if key in result["change_stats"]:
                            change_stats[key] += result["change_stats"][key]

                except Exception as e:
                    log.error(f"Error fetching events for subscription {sub_id}: {e}")

        # Sort events by start time and limit to recent events
        all_events.sort(key=lambda x: x.start_at)
        cutoff_date = datetime.now(timezone.utc) - timedelta(
            days=1
        )  # Show events from yesterday onwards

        filtered_events = [
            e
            for e in all_events
            if datetime.fromisoformat(e.start_at.replace("Z", "+00:00")) >= cutoff_date
        ]

        # For automatic updates, only include new events
        if check_for_changes:
            new_filtered_events = [
                e
                for e in filtered_events
                if any(e.api_id == new_event.api_id for new_event in all_events)
            ]
            return {
                "events": new_filtered_events[
                    : group.max_events
                ],  # Limit to max_events per group
                "new_events_count": total_new_events,
                "change_stats": change_stats,
            }
        else:
            return {
                "events": filtered_events[
                    : group.max_events
                ],  # Limit to max_events per group
                "new_events_count": total_new_events,
                "change_stats": change_stats,
            }

    async def fetch_events_from_subscription(
        self, subscription: Subscription, check_for_changes: bool = True
    ) -> Dict[str, Any]:
        """
        Fetch events from a Luma calendar subscription using the real API.

        Args:
            subscription: The subscription to fetch events for
            check_for_changes: Whether to check for changes using the database

        Returns:
            Dict with 'events', 'new_events', 'change_stats'
        """
        try:
            # Create a temporary HTTP session for this request
            async with LumaAPIClient() as client:
                # Fetch events from the calendar using its api_id
                events = await client.get_calendar_events(
                    calendar_identifier=subscription.api_id,
                    limit=100,  # Fetch up to 100 events per subscription
                )

                # Convert events to dict format for database operations
                event_dicts = []
                for event in events:
                    event_dict = {
                        "api_id": event.api_id,
                        "calendar_api_id": subscription.api_id,
                        "name": event.name,
                        "start_at": event.start_at,
                        "end_at": event.end_at,
                        "timezone": event.timezone,
                        "event_type": event.event_type,
                        "url": event.url,
                        "last_modified": datetime.now(timezone.utc).isoformat(),
                    }
                    event_dicts.append(event_dict)

                if check_for_changes:
                    # Use database to track changes
                    change_stats = await self.event_db.upsert_events(
                        event_dicts, subscription.api_id
                    )
                    new_events = await self.event_db.get_new_events(
                        subscription.api_id, event_dicts
                    )

                    log.info(
                        f"Successfully fetched {len(events)} events from subscription {subscription.name}. "
                        f"Changes: {change_stats['new_events']} new, {change_stats['updated_events']} updated, "
                        f"{change_stats['deleted_events']} deleted"
                    )

                    return {
                        "events": events,
                        "new_events": new_events,
                        "change_stats": change_stats,
                    }
                else:
                    # Just return all events without change tracking
                    log.info(
                        f"Successfully fetched {len(events)} events from subscription {subscription.name}"
                    )
                    return {
                        "events": events,
                        "new_events": events,  # Treat all as new for manual commands
                        "change_stats": {
                            "new_events": len(events),
                            "updated_events": 0,
                            "deleted_events": 0,
                        },
                    }

        except LumaAPINotFoundError:
            log.error(
                f"Calendar {subscription.slug} not found for subscription {subscription.name}"
            )
            return {"events": [], "new_events": [], "change_stats": {}}

        except LumaAPIRateLimitError:
            log.warning(
                f"Rate limit exceeded while fetching events for subscription {subscription.name}"
            )
            # Return empty list on rate limit - don't crash the entire update process
            return {"events": [], "new_events": [], "change_stats": {}}

        except LumaAPIError as e:
            log.error(
                f"API error while fetching events for subscription {subscription.name}: {e}"
            )
            return {"events": [], "new_events": [], "change_stats": {}}

        except Exception as e:
            log.error(
                f"Unexpected error while fetching events for subscription {subscription.name}: {e}"
            )
            return {"events": [], "new_events": [], "change_stats": {}}

    async def send_events_to_channel(
        self,
        channel_id: int,
        events: List[Event],
        guild: discord.Guild,
        group_name: str,
    ):
        """Send formatted events to a Discord channel."""
        try:
            channel = guild.get_channel(channel_id)
            if not channel:
                log.warning(f"Channel {channel_id} not found in guild {guild.id}")
                return

            if not events:
                return

            embed = discord.Embed(
                title=f"📅 Upcoming Events - {group_name}",
                color=discord.Color.blue(),
                timestamp=datetime.now(timezone.utc),
            )

            description = ""
            for event in events[:10]:  # Limit to 10 events per message
                start_time = datetime.fromisoformat(
                    event.start_at.replace("Z", "+00:00")
                )
                time_str = start_time.strftime("%Y-%m-%d %H:%M UTC")

                description += f"**{event.name}**\n"
                description += f"🕐 {time_str}\n"
                description += f"🔗 {event.url}\n\n"

            embed.description = description

            # Check if we have permission to send messages
            if channel.permissions_for(guild.me).send_messages:
                await channel.send(embed=embed)
            else:
                log.warning(f"No permission to send messages in channel {channel_id}")

        except Exception as e:
            log.error(f"Error sending events to channel {channel_id}: {e}")

    @commands.group(name="luma", invoke_without_command=True)
    @commands.guild_only()
    async def luma_group(self, ctx: commands.Context):
        """Luma Events management commands."""
        if ctx.invoked_subcommand is None:
            embed = discord.Embed(
                title="Luma Events Plugin",
                description="Manage Luma calendar subscriptions and event displays",
                color=discord.Color.blue(),
            )
            embed.add_field(
                name="Commands",
                value="• `subscriptions` - Manage subscriptions\n"
                "• `groups` - Manage channel groups\n"
                "• `config` - Configure update settings",
                inline=False,
            )
            await ctx.send(embed=embed)

    @luma_group.group(name="subscriptions", aliases=["subs"])
    async def subscriptions_group(self, ctx: commands.Context):
        """Manage Luma calendar subscriptions.

        This command group allows you to add, remove, and view your Luma calendar subscriptions.
        Each subscription represents a calendar that events will be fetched from.

        Examples:
        - `[p]luma subscriptions` - View all current subscriptions
        - `[p]luma subscriptions add abc123 my-calendar "Team Events"` - Add a new subscription
        - `[p]luma subscriptions remove abc123` - Remove a subscription
        """
        if ctx.invoked_subcommand is None:
            subscriptions = await self.config.guild(ctx.guild).subscriptions()
            if not subscriptions:
                await ctx.send(
                    "No subscriptions configured. Use `[p]luma subscriptions add` to add one."
                )
                return

            embed = discord.Embed(
                title="Current Subscriptions", color=discord.Color.green()
            )

            for sub_id, sub_data in subscriptions.items():
                subscription = Subscription.from_dict(sub_data)
                embed.add_field(
                    name=subscription.name,
                    value=f"API ID: `{subscription.api_id}`\nSlug: `{subscription.slug}`",
                    inline=False,
                )

            await ctx.send(embed=embed)

    @subscriptions_group.command(name="add")
    @checks.admin_or_permissions(manage_guild=True)
    async def add_subscription(
        self, ctx: commands.Context, api_id: str, slug: str, name: str
    ):
        """Add a new Luma calendar subscription.

        Parameters:
        - api_id: The API ID of the Luma calendar
        - slug: The slug/URL identifier of the calendar
        - name: A friendly name for this subscription
        """
        subscriptions = await self.config.guild(ctx.guild).subscriptions()

        if api_id in subscriptions:
            await ctx.send(f"A subscription with API ID `{api_id}` already exists.")
            return

        subscription = Subscription(
            api_id=api_id,
            slug=slug,
            name=name,
            added_by=ctx.author.id,
            added_at=datetime.now(timezone.utc).isoformat(),
        )

        subscriptions[api_id] = subscription.to_dict()
        await self.config.guild(ctx.guild).subscriptions.set(subscriptions)

        embed = discord.Embed(
            title="Subscription Added",
            description=f"Successfully added subscription: **{name}**",
            color=discord.Color.green(),
        )
        embed.add_field(name="API ID", value=f"`{api_id}`", inline=True)
        embed.add_field(name="Slug", value=f"`{slug}`", inline=True)

        await ctx.send(embed=embed)

    @subscriptions_group.command(name="remove", aliases=["delete", "del"])
    @checks.admin_or_permissions(manage_guild=True)
    async def remove_subscription(self, ctx: commands.Context, api_id: str):
        """Remove a Luma calendar subscription."""
        subscriptions = await self.config.guild(ctx.guild).subscriptions()

        if api_id not in subscriptions:
            await ctx.send(f"No subscription found with API ID `{api_id}`.")
            return

        subscription = Subscription.from_dict(subscriptions[api_id])
        del subscriptions[api_id]
        await self.config.guild(ctx.guild).subscriptions.set(subscriptions)

        embed = discord.Embed(
            title="Subscription Removed",
            description=f"Successfully removed subscription: **{subscription.name}**",
            color=discord.Color.red(),
        )
        await ctx.send(embed=embed)

    @luma_group.group(name="groups")
    async def groups_group(self, ctx: commands.Context):
        """Manage channel groups for displaying events."""
        if ctx.invoked_subcommand is None:
            channel_groups = await self.config.guild(ctx.guild).channel_groups()
            if not channel_groups:
                await ctx.send(
                    "No channel groups configured. Use `create` to create one."
                )
                return

            embed = discord.Embed(
                title="Current Channel Groups", color=discord.Color.blue()
            )

            for group_name, group_data in channel_groups.items():
                group = ChannelGroup.from_dict(group_data)
                channel = ctx.guild.get_channel(group.channel_id)
                channel_name = (
                    channel.name if channel else f"Unknown Channel ({group.channel_id})"
                )

                embed.add_field(
                    name=group_name,
                    value=f"Channel: #{channel_name}\nSubscriptions: {len(group.subscription_ids)}\nMax Events: {group.max_events}",
                    inline=False,
                )

            await ctx.send(embed=embed)

    @groups_group.command(name="create")
    @checks.admin_or_permissions(manage_guild=True)
    async def create_group(
        self,
        ctx: commands.Context,
        name: str,
        channel: discord.TextChannel,
        max_events: int = 10,
        group_timezone: Optional[str] = None,
    ):
        """Create a new channel group for displaying events.

        Parameters:
        - name: Name of the group
        - channel: Discord channel to display events in
        - max_events: Maximum number of events to show (default: 10)
        - group_timezone: Optional timezone for displaying event times (e.g., 'America/New_York')
        """
        channel_groups = await self.config.guild(ctx.guild).channel_groups()

        if name in channel_groups:
            await ctx.send(f"A channel group named `{name}` already exists.")
            return

        group = ChannelGroup(
            name=name,
            channel_id=channel.id,
            subscription_ids=[],
            max_events=max_events,
            created_by=ctx.author.id,
            created_at=datetime.now(timezone.utc).isoformat(),
            timezone=group_timezone,
        )

        channel_groups[name] = group.to_dict()
        await self.config.guild(ctx.guild).channel_groups.set(channel_groups)

        embed = discord.Embed(
            title="Channel Group Created",
            description=f"Successfully created group: **{name}**",
            color=discord.Color.green(),
        )
        embed.add_field(name="Channel", value=channel.mention, inline=True)
        embed.add_field(name="Max Events", value=str(max_events), inline=True)
        if group_timezone:
            embed.add_field(name="Timezone", value=group_timezone, inline=True)
        else:
            embed.add_field(name="Timezone", value="Default (from event)", inline=True)

        await ctx.send(embed=embed)

    @groups_group.command(name="timezone", aliases=["tz"])
    @checks.admin_or_permissions(manage_guild=True)
    async def set_group_timezone(
        self, ctx: commands.Context, group_name: str, timezone: str
    ):
        """Set timezone for a channel group.

        Parameters:
        - group_name: Name of the group to update
        - timezone: Timezone to use (e.g., 'America/New_York', 'UTC')
        """
        channel_groups = await self.config.guild(ctx.guild).channel_groups()

        if group_name not in channel_groups:
            await ctx.send(f"No channel group found named `{group_name}`.")
            return

        group = ChannelGroup.from_dict(channel_groups[group_name])
        group.timezone = timezone

        channel_groups[group_name] = group.to_dict()
        await self.config.guild(ctx.guild).channel_groups.set(channel_groups)

        embed = discord.Embed(
            title="Group Timezone Updated",
            description=f"Updated timezone for group: **{group_name}**",
            color=discord.Color.green(),
        )
        embed.add_field(name="Timezone", value=timezone, inline=True)

        await ctx.send(embed=embed)

    @groups_group.command(name="addsub", aliases=["addsubscription"])
    @checks.admin_or_permissions(manage_guild=True)
    async def add_subscription_to_group(
        self, ctx: commands.Context, group_name: str, subscription_api_id: str
    ):
        """Add a subscription to a channel group."""
        channel_groups = await self.config.guild(ctx.guild).channel_groups()
        subscriptions = await self.config.guild(ctx.guild).subscriptions()

        if group_name not in channel_groups:
            await ctx.send(f"No channel group found named `{group_name}`.")
            return

        if subscription_api_id not in subscriptions:
            await ctx.send(
                f"No subscription found with API ID `{subscription_api_id}`."
            )
            return

        group = ChannelGroup.from_dict(channel_groups[group_name])
        subscription = Subscription.from_dict(subscriptions[subscription_api_id])

        if subscription_api_id in group.subscription_ids:
            await ctx.send(
                f"Subscription `{subscription.name}` is already in group `{group_name}`."
            )
            return

        group.subscription_ids.append(subscription_api_id)
        channel_groups[group_name] = group.to_dict()
        await self.config.guild(ctx.guild).channel_groups.set(channel_groups)

        embed = discord.Embed(
            title="Subscription Added to Group",
            description=f"Added **{subscription.name}** to group **{group_name}**",
            color=discord.Color.green(),
        )
        await ctx.send(embed=embed)

    @groups_group.command(name="removesub", aliases=["removesubscription"])
    @checks.admin_or_permissions(manage_guild=True)
    async def remove_subscription_from_group(
        self, ctx: commands.Context, group_name: str, subscription_api_id: str
    ):
        """Remove a subscription from a channel group."""
        channel_groups = await self.config.guild(ctx.guild).channel_groups()

        if group_name not in channel_groups:
            await ctx.send(f"No channel group found named `{group_name}`.")
            return

        group = ChannelGroup.from_dict(channel_groups[group_name])

        if subscription_api_id not in group.subscription_ids:
            await ctx.send(
                f"Subscription `{subscription_api_id}` is not in group `{group_name}`."
            )
            return

        group.subscription_ids.remove(subscription_api_id)
        channel_groups[group_name] = group.to_dict()
        await self.config.guild(ctx.guild).channel_groups.set(channel_groups)

        embed = discord.Embed(
            title="Subscription Removed from Group",
            description=f"Removed subscription from group **{group_name}**",
            color=discord.Color.red(),
        )
        await ctx.send(embed=embed)

    @luma_group.group(name="config")
    @checks.admin_or_permissions(manage_guild=True)
    async def config_group(self, ctx: commands.Context):
        """Configure Luma plugin settings."""
        if ctx.invoked_subcommand is None:
            guild_config = await self.config.guild(ctx.guild).all()
            global_config = await self.config.all()

            embed = discord.Embed(
                title="Luma Configuration", color=discord.Color.blue()
            )
            embed.add_field(
                name="Update Interval",
                value=f"{global_config['update_interval_hours']} hours (global)",
                inline=True,
            )
            embed.add_field(
                name="Enabled",
                value="Yes" if guild_config["enabled"] else "No",
                inline=True,
            )
            embed.add_field(
                name="Last Update",
                value=global_config["last_update"] or "Never",
                inline=True,
            )

            await ctx.send(embed=embed)

    @config_group.command(name="interval")
    @checks.admin_or_permissions(manage_guild=True)
    async def set_update_interval(self, ctx: commands.Context, hours: int):
        """Set the update interval in hours (1-168)."""
        if not 1 <= hours <= 168:
            await ctx.send("Update interval must be between 1 and 168 hours.")
            return

        await self.config.update_interval_hours.set(hours)

        # Restart the update task with new interval
        await self.start_update_task()

        embed = discord.Embed(
            title="Update Interval Updated",
            description=f"Event updates will now occur every {hours} hours.",
            color=discord.Color.green(),
        )
        await ctx.send(embed=embed)

    @config_group.command(name="enable")
    @checks.admin_or_permissions(manage_guild=True)
    async def enable_updates(self, ctx: commands.Context):
        """Enable automatic event updates for this guild."""
        await self.config.guild(ctx.guild).enabled.set(True)

        embed = discord.Embed(
            title="Updates Enabled",
            description="Automatic event updates are now enabled for this guild.",
            color=discord.Color.green(),
        )
        await ctx.send(embed=embed)

    @config_group.command(name="disable")
    @checks.admin_or_permissions(manage_guild=True)
    async def disable_updates(self, ctx: commands.Context):
        """Disable automatic event updates for this guild."""
        await self.config.guild(ctx.guild).enabled.set(False)

        embed = discord.Embed(
            title="Updates Disabled",
            description="Automatic event updates are now disabled for this guild.",
            color=discord.Color.red(),
        )
        await ctx.send(embed=embed)

    @luma_group.command(name="update")
    @checks.admin_or_permissions(manage_guild=True)
    async def manual_update(self, ctx: commands.Context):
        """Manually trigger an event update for this guild."""
        embed = discord.Embed(
            title="Manual Update",
            description="Starting manual update of events...",
            color=discord.Color.blue(),
        )
        message = await ctx.send(embed=embed)

        try:
            await self.update_guild_events(ctx.guild)

            embed.description = "✅ Manual update completed successfully!"
            embed.color = discord.Color.green()
            await message.edit(embed=embed)

        except Exception as e:
            log.error(f"Manual update failed: {e}")
            embed.description = f"❌ Manual update failed: {str(e)}"
            embed.color = discord.Color.red()
            await message.edit(embed=embed)

    @luma_group.command(name="test")
    @checks.admin_or_permissions(manage_guild=True)
    async def test_subscription(self, ctx: commands.Context, subscription_api_id: str):
        """Test a subscription by fetching events directly from the API."""
        subscriptions = await self.config.guild(ctx.guild).subscriptions()

        if subscription_api_id not in subscriptions:
            await ctx.send(
                f"No subscription found with API ID `{subscription_api_id}`."
            )
            return

        subscription = Subscription.from_dict(subscriptions[subscription_api_id])

        embed = discord.Embed(
            title="Testing Subscription",
            description=f"Testing subscription: **{subscription.name}**",
            color=discord.Color.blue(),
        )
        message = await ctx.send(embed=embed)

        try:
            result = await self.fetch_events_from_subscription(subscription)

            if result["events"]:
                embed.title = "✅ Test Successful"
                embed.description = f"Successfully fetched {len(result['events'])} events from **{subscription.name}**"
                embed.color = discord.Color.green()

                # Show first few events
                for i, event in enumerate(result["events"][:3]):
                    start_time = datetime.fromisoformat(
                        event.start_at.replace("Z", "+00:00")
                    )
                    time_str = start_time.strftime("%Y-%m-%d %H:%M UTC")
                    embed.add_field(
                        name=f"{i+1}. {event.name}",
                        value=f"🕐 {time_str}\n🔗 {event.url}",
                        inline=False,
                    )

                if len(result["events"]) > 3:
                    embed.add_field(
                        name="And more...",
                        value=f"{len(result['events']) - 3} more events available",
                        inline=False,
                    )
            else:
                embed.title = "⚠️ No Events Found"
                embed.description = (
                    f"No upcoming events found for **{subscription.name}**"
                )
                embed.color = discord.Color.orange()

            await message.edit(embed=embed)

        except Exception as e:
            log.error(f"Test failed for subscription {subscription_api_id}: {e}")
            embed.title = "❌ Test Failed"
            embed.description = f"Failed to fetch events: {str(e)}"
            embed.color = discord.Color.red()
            await message.edit(embed=embed)

    @luma_group.command(name="cache")
    @checks.admin_or_permissions(manage_guild=True)
    async def cache_info(self, ctx: commands.Context):
        """Display API cache statistics.

        This command shows information about the API request caching system
        used to optimize performance and respect rate limits.

        Example:
        `[p]luma cache` - View cache statistics
        """
        embed = discord.Embed(title="API Cache Statistics", color=discord.Color.blue())

        # This would need access to the client instance
        # For now, provide basic info
        embed.add_field(name="Cache TTL", value="5 minutes", inline=True)
        embed.add_field(name="Auto-cleanup", value="Enabled", inline=True)
        embed.add_field(name="Rate Limiting", value="1 second delay", inline=True)

        await ctx.send(embed=embed)

    @luma_group.command(name="reset")
    @checks.admin_or_permissions(manage_guild=True)
    async def reset_data(self, ctx: commands.Context):
        """Delete all Luma data for this server.

        This command will remove all subscriptions, channel groups, and configuration
        for this Discord server. This action cannot be undone.

        Use this command if you want to completely remove all data stored by this cog
        for GDPR compliance or if you want to start fresh with the configuration.

        Example:
        `[p]luma reset` - Delete all Luma data for this server
        """
        embed = discord.Embed(
            title="⚠️ Reset All Data",
            description="This will permanently delete ALL Luma data for this server including:\n"
            "• All calendar subscriptions\n"
            "• All channel groups\n"
            "• All configuration settings\n\n"
            "**This action cannot be undone.**",
            color=discord.Color.red(),
        )
        embed.add_field(
            name="Confirmation Required",
            value="React with ✅ to confirm or ❌ to cancel.",
            inline=False,
        )

        message = await ctx.send(embed=embed)
        await message.add_reaction("✅")
        await message.add_reaction("❌")

        def check(reaction, user):
            return (
                user == ctx.author
                and reaction.message.id == message.id
                and reaction.emoji in ["✅", "❌"]
            )

        try:
            reaction, user = await self.bot.wait_for(
                "reaction_add", timeout=60.0, check=check
            )

            if reaction.emoji == "✅":
                # Clear all configuration
                await self.config.guild(ctx.guild).clear()

                embed.title = "✅ Data Reset Complete"
                embed.description = (
                    "All Luma data has been permanently deleted for this server."
                )
                embed.color = discord.Color.green()
                await message.edit(embed=embed)

                log.info(
                    f"Luma data reset for guild {ctx.guild.id} by user {ctx.author.id}"
                )

            else:
                embed.title = "❌ Reset Cancelled"
                embed.description = "Data reset was cancelled. No changes were made."
                embed.color = discord.Color.blue()
                await message.edit(embed=embed)

        except asyncio.TimeoutError:
            embed.title = "⏰ Reset Cancelled"
            embed.description = "Confirmation timed out. No changes were made."
            embed.color = discord.Color.orange()
            await message.edit(embed=embed)

    @luma_group.command(name="events")
    @commands.guild_only()
    async def show_events(self, ctx: commands.Context):
        """
        Display upcoming events with improved formatting and actual URLs.

        Shows events from all subscriptions with:
        - Human-readable date/time formatting
        - Actual event URLs instead of just slugs
        - Event type and timezone information
        - Better overall formatting

        Example:
        `[p]luma events` - Show upcoming events with detailed formatting
        """
        subscriptions = await self.config.guild(ctx.guild).subscriptions()

        if not subscriptions:
            await ctx.send(
                "No subscriptions configured. Use `luma subscriptions add` to add one."
            )
            return

        embed = discord.Embed(
            title="📅 Upcoming Events",
            description="Here's what we have coming up:",
            color=discord.Color.blue(),
            timestamp=datetime.now(timezone.utc),
        )

        all_events = []

        try:
            async with LumaAPIClient() as client:
                for sub_id, sub_data in subscriptions.items():
                    subscription = Subscription.from_dict(sub_data)
                    try:
                        events = await client.get_calendar_events(
                            calendar_identifier=subscription.api_id,
                            limit=20,  # Fetch more events for manual display
                        )

                        # Add subscription info to each event
                        for event in events:
                            event.subscription_name = subscription.name
                            all_events.append(event)

                    except Exception as e:
                        log.error(
                            f"Error fetching events for subscription {subscription.name}: {e}"
                        )
                        continue

            if not all_events:
                embed.description = "No upcoming events found."
                await ctx.send(embed=embed)
                return

            # Sort events by start time
            all_events.sort(key=lambda x: x.start_at)

            # Limit to recent events (next 30 days)
            cutoff_date = datetime.now(timezone.utc) + timedelta(days=30)
            recent_events = [
                event
                for event in all_events
                if datetime.fromisoformat(event.start_at.replace("Z", "+00:00"))
                <= cutoff_date
            ]

            if not recent_events:
                embed.description = "No events in the next 30 days."
                await ctx.send(embed=embed)
                return

            # Display events in embed
            for event in recent_events[:10]:  # Limit to 10 events per message
                try:
                    start_time = datetime.fromisoformat(
                        event.start_at.replace("Z", "+00:00")
                    )
                    end_time = (
                        datetime.fromisoformat(event.end_at.replace("Z", "+00:00"))
                        if event.end_at
                        else None
                    )

                    # Format date/time nicely
                    date_str = start_time.strftime("%A, %B %d, %Y")
                    time_str = start_time.strftime("%I:%M %p")

                    if end_time:
                        end_time_str = end_time.strftime("%I:%M %p")
                        time_display = f"{time_str} - {end_time_str}"
                    else:
                        time_display = time_str

                    # Create event title with subscription
                    event_title = f"**{event.name}**"
                    if hasattr(event, "subscription_name"):
                        event_title += f"\n*from {event.subscription_name}*"

                    # Event details
                    details = f"📅 {date_str}\n🕐 {time_display}"

                    if event.timezone:
                        details += f"\n🌍 Timezone: {event.timezone}"

                    if event.event_type:
                        details += f"\n📋 Type: {event.event_type.title()}"

                    # Add location if available
                    if hasattr(event, "geo_address_info") and event.geo_address_info:
                        location = event.geo_address_info
                        if hasattr(location, "city_state") and location.city_state:
                            details += f"\n📍 {location.city_state}"

                    # Add actual URL instead of just slug
                    if event.url:
                        details += f"\n🔗 [View Event]({event.url})"

                    embed.add_field(
                        name=event_title,
                        value=details,
                        inline=False,
                    )

                except Exception as e:
                    log.warning(f"Error formatting event {event.name}: {e}")
                    continue

            if len(recent_events) > 10:
                embed.add_field(
                    name="And more...",
                    value=f"{len(recent_events) - 10} more events available",
                    inline=False,
                )

            await ctx.send(embed=embed)

        except Exception as e:
            log.error(f"Error in show_events command: {e}")
            await ctx.send(f"❌ Failed to fetch events: {str(e)}")
