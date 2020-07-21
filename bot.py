import discord
from discord.ext import commands
import json
import logging
import sys
import mysql.connector
from mysql.connector import Error
import atexit
import streak

logging.basicConfig(format='%(asctime)s [%(levelname)s] %(message)s', stream=sys.stderr,level=logging.INFO)

@atexit.register
def cleanup():
    db_conn.close()

def create_db_connection(host_name, user_name, user_password, db):
    connection = None
    try:
        connection = mysql.connector.connect(
            database=db,
            host=host_name,
            user=user_name,
            passwd=user_password
        )
        logging.info('Connection to MySQL DB successful')
    except Error as e:
        logging.exception(f'The error {e} occurred')
        sys.exit(1)
    return connection

try:
    with open('creds.json', 'r') as file:
        creds = json.load(file)
except:
    logging.exception('Could not load credentials file')
    sys.exit(1)

bot = commands.Bot(command_prefix='$')
db_conn = create_db_connection('localhost', creds['mysql']['user'], creds['mysql']['pass'], 'streakbot')

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
    #user = discord.utils.get(ctx.message.server.members, id = '149435714684059648')
    #await ctx.send(f'*heavy breathing*, you called, {ctx.message.author.name}? Your ID is {ctx.message.author.id}. This is a test ping with the ID <@!149435714684059648>')
    await ctx.send(f'Pong! Latency is {round(bot.latency, 1)}')


@bot.event
async def on_command_error(ctx, error):
    if isinstance(error, commands.CommandOnCooldown):
        min, sec = divmod(error.retry_after, 60)
        hour, min = divmod(min, 60)
        await ctx.send(f'Try again in {int(hour)} hours, {int(min)} minutes, and {int(sec)} seconds')
    else:
        raise error  # re-raise the error so all the errors will still show up in console


@bot.command()
@commands.cooldown(1, 86400, commands.BucketType.user)
# Will throw CommandOnCooldown error if on CD
async def daily(ctx):
    if streak.check_user_exists(ctx.message.author.id, ctx.message.author, db_conn):
        streak_success = streak.set_streak(ctx.message.author.id, db_conn)
        if streak_success is True:
            await ctx.send(f'Daily set for {ctx.message.author} - your current streak is {streak.get_streak(ctx.message.author.id, db_conn)}')
        elif streak_success is False:
            await ctx.send('More than 48 hours have passed since you last claimed your daily, your streak has been reset to 1')
        else:
            await ctx.send('Something went wrong setting your daily, try again later')
    else:
        daily.reset_cooldown(ctx)
        await ctx.send(f'Could not update daily for {ctx.message.author}')

@bot.command()
async def leaderboard(ctx):
    streak.timeout_streaks(db_conn)
    leaderboard = streak.get_leaderboard(db_conn)
    counter = 1
    leaderboard_text = ''
    for user, curr_streak in leaderboard:
        username = bot.get_user(int(user))
        leaderboard_text += f'**{counter}.** {username}  -  {curr_streak}\n'
        counter += 1

    embed = discord.Embed(color=0x00bfff)
    #embed.set_author(name='Streak Leaderboard',icon_url=ctx.guild.icon_url)
    embed.set_thumbnail(url=ctx.guild.icon_url)
    embed.add_field(name='Streak Leaderboard', value=leaderboard_text, inline=True)
    embed.set_footer(text=f'Increase your streak by drawing each day and using {bot.command_prefix}daily!')
    await ctx.send(embed=embed)

    #embed.add_field(name="Field1", value="hi", inline=False)
    #embed.add_field(name="Field2", value="hi2", inline=False)
    #await message.channel.send(embed=embed)
try:
    bot.run(creds['discord']['token'])
except:
    logging.exception('Could not connect to Discord')
    sys.exit(1)
