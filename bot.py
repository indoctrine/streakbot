import discord
from discord.ext import commands
import json
import logging
import sys
import atexit
import time
import asyncio
import configparser
import re
from datetime import datetime
from db import Database

logging.basicConfig(format='%(asctime)s [%(levelname)s] %(message)s', stream=sys.stderr,level=logging.INFO)

config = configparser.ConfigParser()
try:
    config.read('config.ini')
except Exception as e:
    sys.exit(f'Could not load configuration file {e}')
creds = config['Credentials']
db = config['Database']
streakcfg = config['Streaks']

intents = discord.Intents.default()
intents.message_content = True
intents.members = True # Intent allows us to get users that haven't been seen yet

class Streakbot(commands.Bot):
    def __init__(self, **kwargs):
        super(Streakbot, self).__init__(**kwargs)
        self.database = Database(db['Host'], creds['DatabaseUser'], creds['DatabasePass'], db['Name'], int(db['PoolSize']))
        asyncio.get_event_loop().run_until_complete(self.database.bootstrap_db())
        self.db_pool = asyncio.get_event_loop().run_until_complete(self.database.create_pool())

    async def generate_leaderboard(self, title, stats, colour, thumbnail, footer):
        '''Helper function to generate embeds for leaderboards - gracefully handles
        users no longer being on the server.'''
        counter = 1
        leaderboard_text = ''
        for user, stat in stats:
            username = self.get_user(int(user))
            if username is not None:
                leaderboard_text += f'**{counter}.** {username}  -  {stat}\n'
                counter += 1
        embed = discord.Embed(color=colour)
        embed.set_thumbnail(url=thumbnail)
        embed.add_field(name=title, value=leaderboard_text, inline=True)
        embed.set_footer(text=footer)
        return embed

bot = Streakbot(command_prefix='$', case_insensitive=True, intents=intents)
bot.CMD_COOLDOWN = int(streakcfg['Cooldown']) # Cooldown is 23 hours (82800)
bot.STREAK_TIMEOUT = int(streakcfg['Timeout']) # Timeout after 48 hours (172800)
bot.REMINDER_THRESHOLD = int(streakcfg['Reminder']) # Threshold for reminders

if not db['Name'].isalnum():
    logging.exception('Invalid characters in SQL database name')
    sys.exit(1)

@atexit.register
def cleanup():
    logging.info('Shutting down...')

@bot.event
async def on_ready():
    # Load Modules #
    extensions = ['cogs.streak', 'cogs.log', 'cogs.fun', 'cogs.utility']
    for extension in extensions:
        await bot.load_extension(extension)
    logging.info(f'Logged on as {bot.user}!')


@bot.event
async def on_message(message):
    await bot.process_commands(message)
    # To implement message sending command when DM'd

try:
    bot.run(creds['DiscordToken'])
except Exception as e:
    logging.exception(f'Could not connect to Discord {e}')
    sys.exit(1)
