import discord
from discord.ext import commands
import json
import logging
import sys
import atexit
import time
import asyncio
from datetime import datetime
from streak import Streaks
from db import Database

CMD_COOLDOWN = 82800 # Cooldown is 23 hours (82800)
STREAK_TIMEOUT = 172800 # Timeout after 48 hours (172800)
REMINDER_THRESHOLD = 3600 # Threshold for reminders
DB_NAME = 'streakbot' # Must be SQL friendly
DB_HOST = 'localhost'
DB_POOL_SIZE = 10
CREDS_LOCATION = 'creds.json'
reminders = {}

logging.basicConfig(format='%(asctime)s [%(levelname)s] %(message)s', stream=sys.stderr,level=logging.INFO)

if not DB_NAME.isalnum():
    logging.exception('Invalid characters in SQL database name')
    sys.exit(1)

@atexit.register
def cleanup():
    logging.info('Shutting down...')

try:
    with open(CREDS_LOCATION, 'r') as file:
        creds = json.load(file)
except:
    logging.exception('Could not load credentials file')
    sys.exit(1)

intents = discord.Intents.default()
intents.members = True # Intent allows us to get users that haven't been seen yet
bot = commands.Bot(command_prefix='$', case_insensitive=True, intents=intents)
db_pool = Database(DB_HOST, creds['mysql']['user'], creds['mysql']['pass'], DB_NAME, DB_POOL_SIZE)

# Load Modules #
streak = Streaks(db_pool, CMD_COOLDOWN, STREAK_TIMEOUT)

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
        if streak.check_user_exists(ctx.message.author.id, ctx.message.author):
            streak_success = streak.set_streak(ctx.message.author.id)
            if streak_success is True:
                await ctx.send(f'Daily updated for {ctx.message.author} - your current streak is {streak.get_streak(ctx.message.author.id)}')
            elif streak_success is False:
                await ctx.send(f'More than 48 hours have passed, {ctx.message.author}\'s streak has been set to 1')
            else:
                await ctx.send('Something went wrong setting your daily, try again later')
        else:
            await ctx.send(f'Could not update daily for {ctx.message.author}')

    @daily.error
    async def daily_error(self, ctx, error):
        if isinstance(error, commands.CommandOnCooldown):
            min, sec = divmod(error.retry_after, 60)
            hour, min = divmod(min, 60)
            user_id = ctx.message.author.id
            await ctx.send(f'Try again in {int(hour)} hours, {int(min)} minutes, and {int(sec)} seconds')
            if error.retry_after <= 3600:
                print("Time less than an hour")
                if user_id not in reminders:
                    print("User ID not in reminders")
                    reminders[user_id] = error.retry_after
                    await asyncio.sleep(error.retry_after)
                    await ctx.send(f"Hey <@!{user_id}>, it\'s time to claim your daily")
                    reminders.pop(user_id)
        else:
            raise error

    @commands.command(help='''Displays the current leaderboard for daily streak. Streaks
    are timed out when this command is run to ensure all information is up to date.''',
    brief='Displays current streak leaderboard')
    async def leaderboard(self, ctx):
        streak.timeout_streaks()
        leaderboard = streak.get_leaderboard()
        counter = 1
        leaderboard_text = ''
        for user, curr_streak in leaderboard:
            username = self.bot.get_user(int(user))
            if username is not None:
                leaderboard_text += f'**{counter}.** {username}  -  {curr_streak}\n'
                counter += 1
            if counter > 10:
                break

        embed = discord.Embed(color=0x00bfff)
        embed.set_thumbnail(url=ctx.guild.icon_url)
        embed.add_field(name='Streak Leaderboard', value=leaderboard_text, inline=True)
        embed.set_footer(text=f'Increase your streak by drawing each day and using {bot.command_prefix}daily!')
        await ctx.send(embed=embed)


    @commands.command(help=f'''Displays the personal best leaderboard for daily streak.
    This leaderboard shows the best unbroken streaks of all time when run with {bot.command_prefix}pb.
    To look up other years, run {bot.command_prefix}pb <year>.
    You can check the personal best of an individual user using {bot.command_prefix}pb <year> <user>''',
    brief='Displays personal best leaderboard')
    async def pb(self, ctx, year: int = 0, user: discord.Member = None):
        if year > datetime.now().year:
            raise commands.CommandError
        if user is None:
            personal_best = streak.get_pb_leaderboard(year)
            counter = 1
            pb_leaderboard_text = ''
            for user, pb in personal_best:
                username = self.bot.get_user(int(user))
                if username is not None:
                    pb_leaderboard_text += f'**{counter}.** {username}  -  {pb}\n'
                    counter += 1
                if counter > 10:
                    break

            embed = discord.Embed(color=0x00bfff)
            embed.set_thumbnail(url=ctx.guild.icon_url)
            embed.add_field(name=f'{year if year > 0 else "All Time"} Personal Best Leaderboard',
                            value=pb_leaderboard_text, inline=True)
            embed.set_footer(text=f'Set new records by drawing each day and using {bot.command_prefix}daily!')
            await ctx.send(embed=embed)
        else:
            personal_best = streak.get_user_pb(user.id, year)
            if personal_best is not None:
                personal_best = personal_best[0]
                await ctx.send(f'Personal best of {year if year > 0 else "all time"} for {user} is {personal_best}')
            else:
                raise commands.BadArgument

    @pb.error
    async def pb_error(self, ctx, error):
        if isinstance(error, commands.BadArgument):
            await ctx.send('Unable to look up personal best - Invalid argument(s)')
        elif isinstance(error, commands.CommandError):
            await ctx.send(f'Year is in the future, cannot return results')
        else:
            raise error


bot.add_cog(Utility_Commands(bot))
bot.add_cog(Streak_Commands(bot))
bot.add_cog(Fun_Commands(bot))

try:
    bot.run(creds['discord']['token'])
except Exception as e:
    logging.exception(f'Could not connect to Discord {e}')
    sys.exit(1)
