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
from streak import Streaks
from log import Log
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
CMD_COOLDOWN = int(streakcfg['Cooldown']) # Cooldown is 23 hours (82800)
STREAK_TIMEOUT = int(streakcfg['Timeout']) # Timeout after 48 hours (172800)
REMINDER_THRESHOLD = int(streakcfg['Reminder']) # Threshold for reminders
reminders = {}


if not db['Name'].isalnum():
    logging.exception('Invalid characters in SQL database name')
    sys.exit(1)

@atexit.register
def cleanup():
    logging.info('Shutting down...')

intents = discord.Intents.default()
intents.members = True # Intent allows us to get users that haven't been seen yet
bot = commands.Bot(command_prefix='$', case_insensitive=True, intents=intents)


db_pool = Database(db['Host'], creds['DatabaseUser'], creds['DatabasePass'], db['Name'], int(db['PoolSize']))
asyncio.get_event_loop().run_until_complete(db_pool.bootstrap_db())
asyncio.get_event_loop().run_until_complete(db_pool.create_pool())

# Load Modules #
streak = Streaks(db_pool, CMD_COOLDOWN, STREAK_TIMEOUT)
log = Log(db_pool)

@bot.event
async def on_ready():
    logging.info(f'Logged on as {bot.user}!')


@bot.event
async def on_message(message):
    channel = message.channel.id
    await bot.process_commands(message)
    # To implement message sending command when DM'd

class Utility_Commands(commands.Cog, name='Utility Commands'):
    def __init__(self, bot):
        self.bot = bot
        self.bot.help_command.cog = self
    @commands.command(help='Ping the bot to get the current latency',brief='Ping the bot')
    async def ping(self, ctx):
        await ctx.send(f'Pong! Latency is {round(self.bot.latency*1000)}ms')

class Fun_Commands(commands.Cog, name='Fun Commands'):
    def __init__(self, bot):
        self.bot = bot
    @commands.command(help=f'''Hug a user by using {bot.command_prefix}hug <user>.
    The bot will then tag the user for hugs''', brief='Hug a user!')
    async def hug(self, ctx, *, user: discord.Member = None):
        if user is not None:
            await ctx.send(f'Sending hugs to <@!{user.id}> <:takenrg:670936332822118420>')
        else:
            await ctx.send('Hugs for who?')

    @hug.error
    async def hug_error(self, ctx, error):
        if isinstance(error, commands.BadArgument):
            await ctx.send("I don't know who that is.")
        else:
            raise error

class Log_Commands(commands.Cog, name='Log Commands'):
    def __init__(self, bot):
        self.bot = bot
    @commands.command(help='''This command will allow you to log time or pages
    drawn.''', brief='Log time or pages for this month')
    async def log(self, ctx, log_type, amount: int = 0):
        log_type = log_type.lower()
        log_types = ['pages', 'page', 'time']
        if log_type in log_types:
            user_exists = await streak.check_user_exists(ctx.message.author.id, ctx.message.author)
        else:
            raise commands.errors.BadArgument()
        if user_exists:
            if amount == 0:
                raise commands.errors.BadArgument()
            if re.findall('pages?', log_type):
                page_results = await log.log_pages(ctx.message.author.id, amount)
                await ctx.send(f'Updated pages for {ctx.message.author} - current month = {page_results}')
            else:
                print('To be filled with time logging')

    @log.error
    async def log_error(self, ctx, error):
        if isinstance(error, commands.errors.MissingRequiredArgument):
            await ctx.send('Log type is a required argument - valid logging types are `page(s)` or `time`')
        elif isinstance(error, commands.errors.BadArgument):
            await ctx.send('Invalid argument(s), command format is `log {type} {amount}`')

class Streak_Commands(commands.Cog, name='Streak Commands'):
    def __init__(self, bot):
        self.bot = bot
    @commands.command(help='''Running this command will allow you to add to your
    daily streak for drawing each day. This command is on a 23 hour cooldown so
    if your times start to creep, you can slowly bring them back. 48 hours without
    adding to your daily will reset your streak back to 0 (or 1 upon claim)''',
    brief='Add to your drawing streak')
    # Will throw CommandOnCooldown error if on CD
    async def daily(self, ctx):
        user_exists = await streak.check_user_exists(ctx.message.author.id, ctx.message.author)
        if user_exists:
            streak_success = await streak.set_streak(ctx.message.author.id)
            if streak_success['status'] is 'success':
                await ctx.send(f"Daily updated for {ctx.message.author} - your current streak is {streak_success['streak']}")
            elif streak_success['status'] is 'timeout':
                await ctx.send(f"More than 48 hours have passed, {ctx.message.author}\'s streak has been set to {streak_success['streak']}")
            elif streak_success['status'] is 'on_cooldown':
                raise commands.CommandOnCooldown(CMD_COOLDOWN, streak_success['cooldown'])
        else:
            await ctx.send(f'Could not update daily for {ctx.message.author}')

    @daily.error
    async def daily_error(self, ctx, error):
        if isinstance(error, commands.CommandOnCooldown):
            min, sec = divmod(error.retry_after, 60)
            hour, min = divmod(min, 60)
            user_id = ctx.message.author.id
            await ctx.send(f'Try again in {int(hour)} hours, {int(min)} minutes, and {int(sec)} seconds')
            if error.retry_after <= REMINDER_THRESHOLD:
                if user_id not in reminders:
                    reminders[user_id] = error.retry_after
                    await asyncio.sleep(error.retry_after)
                    await ctx.send(f"Hey <@!{user_id}>, it\'s time to claim your daily")
                    reminders.pop(user_id)
        else:
            raise error

    @commands.command(help='''Displays the current leaderboard for daily streak. Streaks
    are timed out when this command is run to ensure all information is up to date. This
    command can take the `current` argument to return only this years' leaderboard''',
    brief='Displays current streak leaderboard')
    async def leaderboard(self, ctx, arg = 'overall'):
        if arg.lower() == 'current':
            leaderboard_type = 'Current Year Streak Leaderboard'
            leaderboard = await streak.get_leaderboard(arg)
        else:
            leaderboard_type = 'Overall Streak Leaderboard'
            leaderboard = await streak.get_leaderboard()
        counter = 1
        leaderboard_text = ''
        for user, curr_streak in leaderboard:
            username = self.bot.get_user(int(user))
            if username is not None:
                leaderboard_text += f'**{counter}.** {username}  -  {curr_streak}\n'
                counter += 1

        embed = discord.Embed(color=0x00bfff)
        embed.set_thumbnail(url=ctx.guild.icon_url)
        embed.add_field(name=leaderboard_type, value=leaderboard_text, inline=True)
        embed.set_footer(text=f'Increase your streak by drawing each day and using {bot.command_prefix}daily!')
        await ctx.send(embed=embed)


    @commands.command(help=f'''Displays the personal best leaderboard for daily streak.
    This leaderboard shows the best unbroken streaks of all time when run with {bot.command_prefix}pb.
    To look up other years, run {bot.command_prefix}pb <year>.
    You can check the personal best of an individual user using {bot.command_prefix}pb <year> <user>''',
    brief='Displays personal best leaderboard')
    async def pb(self, ctx, year: int = 0, user: discord.Member = None):
        current_year = datetime.now().year
        if year > current_year:
            await ctx.send(f'Year is in the future, please enter a valid year')
            return False
        if user is None:
            personal_best = await streak.get_pb_leaderboard(year)
            if personal_best:
                counter = 1
                pb_leaderboard_text = ''
                for user, pb in personal_best:
                    username = self.bot.get_user(int(user))
                    if username is not None:
                        pb_leaderboard_text += f'**{counter}.** {username}  -  {pb}\n'
                        counter += 1

                embed = discord.Embed(color=0x00bfff)
                embed.set_thumbnail(url=ctx.guild.icon_url)
                embed.add_field(name=f'{year if year > 0 else "All Time"} Personal Best Leaderboard',
                                value=pb_leaderboard_text, inline=True)
                embed.set_footer(text=f'Set new records by drawing each day and using {bot.command_prefix}daily!')
                await ctx.send(embed=embed)
                return True
            else:
                await ctx.send(f'No valid leaderboard for {year}')
                return False
        else:
            personal_best = await streak.get_user_pb(user.id, year)
            if personal_best is not None:
                personal_best = personal_best[0]
                await ctx.send(f'Personal best of {year if year > 0 else "all time"} for {user} {"was" if year < current_year else "is"} {personal_best}')
                return True
            else:
                raise commands.BadArgument

    @pb.error
    async def pb_error(self, ctx, error):
        if isinstance(error, commands.BadArgument):
            await ctx.send(f'Invalid arguments \n Usage: `{bot.command_prefix}pb <year> <user>`')
        else:
            raise error


bot.add_cog(Utility_Commands(bot))
bot.add_cog(Streak_Commands(bot))
bot.add_cog(Log_Commands(bot))
bot.add_cog(Fun_Commands(bot))

try:
    bot.run(creds['DiscordToken'])
except Exception as e:
    logging.exception(f'Could not connect to Discord {e}')
    sys.exit(1)
