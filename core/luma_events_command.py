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
            await ctx.send("No subscriptions configured. Use `luma subscriptions add` to add one.")
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
                        log.error(f"Error fetching events for subscription {subscription.name}: {e}")
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
                event for event in all_events
                if datetime.fromisoformat(event.start_at.replace("Z", "+00:00")) <= cutoff_date
            ]
            
            if not recent_events:
                embed.description = "No events in the next 30 days."
                await ctx.send(embed=embed)
                return
            
            # Display events in embed
            for event in recent_events[:10]:  # Limit to 10 events per message
                try:
                    start_time = datetime.fromisoformat(event.start_at.replace("Z", "+00:00"))
                    end_time = datetime.fromisoformat(event.end_at.replace("Z", "+00:00")) if event.end_at else None
                    
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
                    if hasattr(event, 'subscription_name'):
                        event_title += f"\n*from {event.subscription_name}*"
                    
                    # Event details
                    details = f"📅 {date_str}\n🕐 {time_display}"
                    
                    if event.timezone:
                        details += f"\n🌍 Timezone: {event.timezone}"
                    
                    if event.event_type:
                        details += f"\n📋 Type: {event.event_type.title()}"
                    
                    # Add location if available
                    if hasattr(event, 'geo_address_info') and event.geo_address_info:
                        location = event.geo_address_info
                        if location.city_state:
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