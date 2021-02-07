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
intents.members = True # Intent allows us to get users that haven't been seen yet
bot = commands.Bot(command_prefix='$', case_insensitive=True, intents=intents)

bot.CMD_COOLDOWN = int(streakcfg['Cooldown']) # Cooldown is 23 hours (82800)
bot.STREAK_TIMEOUT = int(streakcfg['Timeout']) # Timeout after 48 hours (172800)
bot.REMINDER_THRESHOLD = int(streakcfg['Reminder']) # Threshold for reminders

if not db['Name'].isalnum():
    logging.exception('Invalid characters in SQL database name')
    sys.exit(1)
else:
    database = Database(db['Host'], creds['DatabaseUser'], creds['DatabasePass'], db['Name'], int(db['PoolSize']))
    asyncio.get_event_loop().run_until_complete(database.bootstrap_db())
    bot.db_pool = asyncio.get_event_loop().run_until_complete(database.create_pool())

@atexit.register
def cleanup():
    logging.info('Shutting down...')

# Load Modules #
extensions = ['cogs.streak', 'cogs.log', 'cogs.fun', 'cogs.utility']

if __name__ == '__main__':
    for extension in extensions:
        bot.load_extension(extension)

@bot.event
async def on_ready():
    logging.info(f'Logged on as {bot.user}!')


@bot.event
async def on_message(message):
    channel = message.channel.id
    await bot.process_commands(message)
    # To implement message sending command when DM'd

try:
    bot.run(creds['DiscordToken'])
except Exception as e:
    logging.exception(f'Could not connect to Discord {e}')
    sys.exit(1)
