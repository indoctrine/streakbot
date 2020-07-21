import discord
from discord.ext import commands
import json
import logging
import sys
import mysql.connector
from mysql.connector import Error
from mysql.connector import pooling
import atexit
from streak import Streaks

CMD_COOLDOWN = 86400 # Default cooldown is 24 hours
DB_NAME = 'streakbot'

logging.basicConfig(format='%(asctime)s [%(levelname)s] %(message)s', stream=sys.stderr,level=logging.INFO)

@atexit.register
def cleanup():
    logging.info('Shutting down...')

def create_db_connpool(host_name, user_name, user_password, db):
    connection_pool = None
    try:
        connection_pool = mysql.connector.pooling.MySQLConnectionPool(
            pool_name="streakbot_pool",
            pool_size=5,
            pool_reset_session=True,
            host=host_name,
            database=db,
            user=user_name,
            password=user_password)
        logging.info('Connection to MySQL DB successful')
        return connection_pool
    except Error as e:
        logging.exception(f'Error while connecting to MySQL using Connection pool {e}')
        sys.exit(1)

try:
    with open('creds.json', 'r') as file:
        creds = json.load(file)
except:
    logging.exception('Could not load credentials file')
    sys.exit(1)

bot = commands.Bot(command_prefix='$')
db_pool = create_db_connpool('localhost', creds['mysql']['user'], creds['mysql']['pass'], DB_NAME)
streak = Streaks(db_pool, CMD_COOLDOWN)

@bot.event
async def on_ready():
    logging.info(f'Logged on as {bot.user}!')


@bot.event
async def on_message(message):
    await bot.process_commands(message)
    channel = message.channel
    print('Message from {0.author}: {0.content}'.format(message))
    if message.content.startswith('beep'):
        await channel.send('Beep boop')


@bot.command()
async def ping(ctx):
    await ctx.send(f'Pong! Latency is {round(bot.latency, 1)}')


@bot.event
async def on_command_error(ctx, error):
    error = getattr(error, "original", error)
    if isinstance(error, commands.CommandOnCooldown):
        min, sec = divmod(error.retry_after, 60)
        hour, min = divmod(min, 60)
        await ctx.send(f'Try again in {int(hour)} hours, {int(min)} minutes, and {int(sec)} seconds')
    else:
        raise error  # re-raise the error so all the errors will still show up in console


@bot.command()
async def hug(ctx, arg: discord.Member = None):
    if arg is not None:
        await ctx.send(f'Sending hugs to <@!{arg.id}> <:takenrg:670936332822118420>')
    else:
        await ctx.send('Hugs for who?')


@hug.error
async def hug_error(ctx, error):
    if isinstance(error, commands.BadArgument):
        await ctx.send("I don't know who that is.")
    else:
        raise error


@bot.command()
# Will throw CommandOnCooldown error if on CD
async def daily(ctx):
    if streak.check_user_exists(ctx.message.author.id, ctx.message.author):
        streak_success = streak.set_streak(ctx.message.author.id)
        if streak_success is True:
            await ctx.send(f'Daily set for {ctx.message.author} - your current streak is {streak.get_streak(ctx.message.author.id)}')
        elif streak_success is False:
            await ctx.send('More than 48 hours have passed since you last claimed your daily, your streak has been reset to 1')
        else:
            await ctx.send('Something went wrong setting your daily, try again later')
    else:
        await ctx.send(f'Could not update daily for {ctx.message.author}')


@bot.command()
async def leaderboard(ctx):
    streak.timeout_streaks()
    leaderboard = streak.get_leaderboard()
    counter = 1
    leaderboard_text = ''
    for user, curr_streak in leaderboard:
        username = bot.get_user(int(user))
        leaderboard_text += f'**{counter}.** {username}  -  {curr_streak}\n'
        counter += 1

    embed = discord.Embed(color=0x00bfff)
    embed.set_thumbnail(url=ctx.guild.icon_url)
    embed.add_field(name='Streak Leaderboard', value=leaderboard_text, inline=True)
    embed.set_footer(text=f'Increase your streak by drawing each day and using {bot.command_prefix}daily!')
    await ctx.send(embed=embed)


@bot.command()
async def pb(ctx, arg: discord.Member = None):
    if arg is None:
        personal_best = streak.get_pb_leaderboard()
        counter = 1
        pb_leaderboard_text = ''
        for user, pb in personal_best:
            username = bot.get_user(int(user))
            pb_leaderboard_text += f'**{counter}.** {username}  -  {pb}\n'
            counter += 1

        embed = discord.Embed(color=0x00bfff)
        embed.set_thumbnail(url=ctx.guild.icon_url)
        embed.add_field(name='Personal Best Leaderboard',
                        value=pb_leaderboard_text, inline=True)
        embed.set_footer(text=f'Set new records by drawing each day and using {bot.command_prefix}daily!')
        await ctx.send(embed=embed)
    else:
        personal_best = streak.get_user_pb(arg.id)[0]
        await ctx.send(f'Current personal best for {arg} is {personal_best}')


@pb.error
async def pb_error(ctx, error):
    if isinstance(error, commands.BadArgument):
        await ctx.send("Unable to look up personal best for that user.")
    else:
        raise error

try:
    bot.run(creds['discord']['token'])
except Exception as e:
    logging.exception(f'Could not connect to Discord {e}')
    sys.exit(1)
