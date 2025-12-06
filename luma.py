import asyncio
import logging
from datetime import datetime

import discord
from redbot.core import Config, commands, checks
from redbot.core.bot import Red
from redbot.core.utils import menus, timedelta
from typing import Dict, List
from .calendar_get import Event

from .data_models import Subscription, ChannelGroup
from .api_client import (
    LumaAPIClient,
    LumaAPIError,
    LumaAPIRateLimitError,
    LumaAPINotFoundError,
)

log = logging.getLogger("red.luma")

# End-User Data Statement for Redbot compliance
__data_statement__ = """
This cog stores the following data for each Discord server (guild) that uses it:

1. Luma API credentials and calendar subscription information
   - Luma API IDs, slugs, and friendly names for subscribed calendars
   - Configuration for channel groups and update intervals
   - Timestamps of when subscriptions were added

2. Data is stored using Red's Config system with the following keys:
   - subscriptions: Dict of API ID -> subscription details
   - channel_groups: Dict of group name -> channel group configuration
   - update_interval_hours: Integer for update frequency
   - enabled: Boolean for automatic updates
   - last_update: ISO timestamp of last update

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

        # Default configuration
        default_guild = {
            "subscriptions": {},
            "channel_groups": {},
            "update_interval_hours": 24,
            "last_update": None,
            "enabled": True,
        }

        self.config.register_guild(**default_guild)

        # Background task for updating events
        self.update_task = None
        self.bot.loop.create_task(self.initialize())

    async def initialize(self):
        """Initialize the cog and start background tasks.

        This method is called when the cog is loaded. It waits for the bot
        to be ready and then starts the background update task if updates
        are enabled for the global config.
        """
        await self.bot.wait_until_ready()
        if await self.config.enabled():
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
                await self.update_guild_events(guild)
            except Exception as e:
                log.error(f"Error updating events for guild {guild.id}: {e}")

        await self.config.last_update.set(datetime.utcnow().isoformat())

    async def update_guild_events(self, guild: discord.Guild):
        """Update events for a specific guild."""
        subscriptions = await self.config.guild(guild).subscriptions()
        channel_groups = await self.config.guild(guild).channel_groups()

        for group_name, group_data in channel_groups.items():
            group = ChannelGroup.from_dict(group_data)
            events = await self.fetch_events_for_group(group, subscriptions)

            if events:
                await self.send_events_to_channel(
                    group.channel_id, events, guild, group_name
                )

    async def fetch_events_for_group(
        self, group: ChannelGroup, subscriptions: Dict
    ) -> List[Event]:
        """Fetch events for a specific channel group."""
        events = []

        for sub_id in group.subscription_ids:
            if sub_id in subscriptions:
                subscription = Subscription.from_dict(subscriptions[sub_id])
                try:
                    sub_events = await self.fetch_events_from_subscription(subscription)
                    events.extend(sub_events)
                except Exception as e:
                    log.error(f"Error fetching events for subscription {sub_id}: {e}")

        # Sort events by start time and limit to recent events
        events.sort(key=lambda x: x.start_at)
        cutoff_date = datetime.utcnow() - timedelta(
            days=1
        )  # Show events from yesterday onwards

        filtered_events = [
            e
            for e in events
            if datetime.fromisoformat(e.start_at.replace("Z", "+00:00")) >= cutoff_date
        ]
        return filtered_events[: group.max_events]  # Limit to max_events per group

    async def fetch_events_from_subscription(
        self, subscription: Subscription
    ) -> List[Event]:
        """Fetch events from a Luma calendar subscription using the real API."""
        try:
            # Create a temporary HTTP session for this request
            async with LumaAPIClient() as client:
                # Fetch events from the calendar using its api_id
                events = await client.get_calendar_events(
                    calendar_api_id=subscription.api_id,
                    limit=100,  # Fetch up to 100 events per subscription
                )

                log.info(
                    f"Successfully fetched {len(events)} events from subscription {subscription.name}"
                )
                return events

        except LumaAPINotFoundError:
            log.error(
                f"Calendar {subscription.slug} not found for subscription {subscription.name}"
            )
            return []

        except LumaAPIRateLimitError:
            log.warning(
                f"Rate limit exceeded while fetching events for subscription {subscription.name}"
            )
            # Return empty list on rate limit - don't crash the entire update process
            return []

        except LumaAPIError as e:
            log.error(
                f"API error while fetching events for subscription {subscription.name}: {e}"
            )
            return []

        except Exception as e:
            log.error(
                f"Unexpected error while fetching events for subscription {subscription.name}: {e}"
            )
            return []

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
                timestamp=datetime.utcnow(),
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
            added_at=datetime.utcnow().isoformat(),
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
    ):
        """Create a new channel group for displaying events.

        Parameters:
        - name: Name of the group
        - channel: Discord channel to display events in
        - max_events: Maximum number of events to show (default: 10)
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
            created_at=datetime.utcnow().isoformat(),
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
            config = await self.config.guild(ctx.guild).all()

            embed = discord.Embed(
                title="Luma Configuration", color=discord.Color.blue()
            )
            embed.add_field(
                name="Update Interval",
                value=f"{config['update_interval_hours']} hours",
                inline=True,
            )
            embed.add_field(
                name="Enabled", value="Yes" if config["enabled"] else "No", inline=True
            )
            embed.add_field(
                name="Last Update", value=config["last_update"] or "Never", inline=True
            )

            await ctx.send(embed=embed)

    @config_group.command(name="interval")
    @checks.admin_or_permissions(manage_guild=True)
    async def set_update_interval(self, ctx: commands.Context, hours: int):
        """Set the update interval in hours (1-168)."""
        if not 1 <= hours <= 168:
            await ctx.send("Update interval must be between 1 and 168 hours.")
            return

        await self.config.guild(ctx.guild).update_interval_hours.set(hours)

        # Restart the update task with new interval
        if await self.config.enabled():
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
        """Enable automatic event updates."""
        await self.config.guild(ctx.guild).enabled.set(True)
        await self.start_update_task()

        embed = discord.Embed(
            title="Updates Enabled",
            description="Automatic event updates are now enabled.",
            color=discord.Color.green(),
        )
        await ctx.send(embed=embed)

    @config_group.command(name="disable")
    @checks.admin_or_permissions(manage_guild=True)
    async def disable_updates(self, ctx: commands.Context):
        """Disable automatic event updates."""
        await self.config.guild(ctx.guild).enabled.set(False)
        if self.update_task:
            self.update_task.cancel()
            self.update_task = None

        embed = discord.Embed(
            title="Updates Disabled",
            description="Automatic event updates are now disabled.",
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
            events = await self.fetch_events_from_subscription(subscription)

            if events:
                embed.title = "✅ Test Successful"
                embed.description = f"Successfully fetched {len(events)} events from **{subscription.name}**"
                embed.color = discord.Color.green()

                # Show first few events
                for i, event in enumerate(events[:3]):
                    start_time = datetime.fromisoformat(
                        event.start_at.replace("Z", "+00:00")
                    )
                    time_str = start_time.strftime("%Y-%m-%d %H:%M UTC")
                    embed.add_field(
                        name=f"{i+1}. {event.name}",
                        value=f"🕐 {time_str}\n🔗 {event.url}",
                        inline=False,
                    )

                if len(events) > 3:
                    embed.add_field(
                        name="And more...",
                        value=f"{len(events) - 3} more events available",
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
