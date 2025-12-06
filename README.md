# Luma Events Cog for Red-DiscordBot

A comprehensive cog for managing Luma calendar subscriptions and displaying events across Discord channels.

## Features

- **Multiple Calendar Subscriptions**: Add and manage multiple Luma calendars per server
- **Channel Groups**: Organize events into different channel displays
- **Automatic Updates**: Background task with configurable update intervals (1-168 hours)
- **Rich Event Display**: Beautiful embeds with event details, times, and links
- **Manual Controls**: Force updates, test subscriptions, and manage configurations
- **GDPR Compliant**: Complete data deletion command for privacy compliance

## Installation

### Using Red's Downloader (Recommended)

1. Add this repository to your Red bot:
   ```
   [p]repo add luma-redbot https://github.com/yourusername/luma-redbot
   ```

2. Install the cog:
   ```
   [p]cog install luma-redbot luma
   ```

3. Load the cog:
   ```
   [p]load luma
   ```

### Manual Installation

1. Clone this repository:
   ```bash
   git clone https://github.com/yourusername/luma-redbot.git
   ```

2. Copy the `luma` folder to your Red's cogs directory:
   ```bash
   cp -r luma /path/to/your/red/cogs/
   ```

3. Load the cog:
   ```
   [p]load luma
   ```

## Setup Instructions

### 1. Get Luma Calendar Information

To add a Luma subscription, you'll need:
- **API ID**: The calendar's API identifier
- **Slug**: The calendar's URL slug
- **Name**: A friendly name for your reference

You can find these by:
1. Visiting your Luma calendar page
2. Looking at the URL: `https://lu.ma/calendar/{slug}`
3. Using browser developer tools to find the API ID

### 2. Configure Your Server

1. **Add a subscription**:
   ```
   [p]luma subscriptions add <api_id> <slug> "Calendar Name"
   ```

2. **Create a channel group**:
   ```
   [p]luma groups create "Event Updates" #events 10
   ```

3. **Link subscription to group**:
   ```
   [p]luma groups addsub "Event Updates" <api_id>
   ```

4. **Enable automatic updates**:
   ```
   [p]luma config enable
   [p]luma config interval 6
   ```

## Commands

### Main Commands
- `[p]luma` - Show help and overview
- `[p]luma update` - Manually trigger event update
- `[p]luma test <api_id>` - Test a subscription
- `[p]luma reset` - Delete all data (GDPR compliance)
- `[p]luma cache` - View cache statistics

### Subscription Management
- `[p]luma subscriptions` - List all subscriptions
- `[p]luma subscriptions add <api_id> <slug> <name>` - Add subscription
- `[p]luma subscriptions remove <api_id>` - Remove subscription

### Channel Group Management
- `[p]luma groups` - List all channel groups
- `[p]luma groups create <name> <channel> [max_events]` - Create group
- `[p]luma groups addsub <group_name> <api_id>` - Add subscription to group
- `[p]luma groups removesub <group_name> <api_id>` - Remove from group

### Configuration
- `[p]luma config` - Show current configuration
- `[p]luma config interval <hours>` - Set update interval (1-168)
- `[p]luma config enable` - Enable automatic updates
- `[p]luma config disable` - Disable automatic updates

## Requirements

- Red-DiscordBot 3.5.0 or higher
- Python 3.8 or higher
- Internet connection for API requests
- Administrator permissions for configuration

### Dependencies
- `aiohttp>=3.8.0` - HTTP client for API requests
- `pydantic>=1.10.0` - Data validation and settings management

## Data Storage & Privacy

This cog stores the following data for each Discord server:
- Luma API credentials and calendar subscription information
- Configuration for channel groups and update intervals
- Timestamps of when subscriptions were added

**No personal user data is collected or stored.**

Data can be deleted at any time using:
- `[p]luma reset` command
- Removing the cog from the server

See our [End-User Data Statement](luma.py) for complete details.

## Troubleshooting

### Common Issues

**"No events found"**
- Check that your API ID and slug are correct
- Verify the calendar is public or accessible
- Test the subscription with `[p]luma test <api_id>`

**"Rate limit exceeded"**
- The cog automatically handles rate limits
- If it persists, increase the update interval
- Check your internet connection

**"Channel not found"**
- Ensure the bot has permissions to view and send messages in the channel
- Verify the channel ID is correct
- Check that the channel still exists

**"API error"**
- Verify your Luma calendar is accessible
- Check that the calendar URL is correct
- Ensure you have the right permissions for the calendar

### Debug Commands

- `[p]luma test <api_id>` - Test a specific subscription
- `[p]luma cache` - View cache and rate limiting info
- `[p]luma config` - Check current configuration

### Logs

Check Red's logs for detailed error messages:
```bash
# View recent logs
[p]log tail
```

Look for entries with `red.luma` in the log level.

## Support

- **Issues**: Report bugs on GitHub Issues
- **Discussions**: Join our Discord server
- **Documentation**: Full command reference in `[p]help luma`

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## License

This project is licensed under the MIT License - see the [LICENSE](../LICENSE) file for details.

## Changelog

### Version 1.0.0
- Initial release
- Basic Luma calendar integration
- Channel groups and automatic updates
- GDPR compliance features