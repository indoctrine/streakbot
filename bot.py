import discord
from discord.ext import commands
import json
import logging
import sys
import mariadb
from mariadb import Error
#import mysql.connector
#from mysql.connector import Error
#from mysql.connector import pooling
import atexit
from streak import Streaks

CMD_COOLDOWN = 82800 # Cooldown is 23 hours
DB_NAME = 'streakbot' # Must be SQL friendly
DB_HOST = 'localhost'

logging.basicConfig(format='%(asctime)s [%(levelname)s] %(message)s', stream=sys.stderr,level=logging.INFO)

@atexit.register
def cleanup():
    logging.info('Shutting down...')

def create_db_connpool(host_name, user_name, user_password, db):
    connection_pool = None
    try:
        if bootstrap_db(user_name, user_password, host_name, db):
            connection_pool = mariadb.ConnectionPool(
                pool_name="streakbot_pool",
                pool_size=5,
                pool_reset_connection=True,
                host=host_name,
                user=user_name,
                password=user_password,
                database=db)
            logging.info('Connection to MySQL DB successful')
            return connection_pool
        else:
            raise Exception('Cannot bootstrap DB')
    except Error as e:
        logging.exception(f'Error while connecting to MySQL using Connection pool {e}')
        sys.exit(1)

def bootstrap_db(user_name, user_password, host_name, db):
    try:
        db_conn = mariadb.connection(
                    user=user_name,
                    password=user_password,
                    host=host_name)
        cursor = db_conn.cursor()
        create_db = 'CREATE DATABASE IF NOT EXISTS {db}'
        cursor.execute(create_db.format(db=DB_NAME))
        db_conn.commit()
        use_db = 'USE {db}'
        cursor.execute(use_db.format(db=DB_NAME))
        create_user_table = '''CREATE TABLE IF NOT EXISTS `users` (
                        	`user_id` VARCHAR(50) NOT NULL,
                        	`username` VARCHAR(50) NOT NULL,
                        	`discriminator` INT NOT NULL,
                        	`daily_claimed` DATETIME,
                        	`streak` INT DEFAULT 0,
                        	`personal_best` INT DEFAULT 0,
                        	PRIMARY KEY (`user_id`)
                        );'''
        cursor.execute(create_user_table)
        create_logs_table = '''CREATE TABLE IF NOT EXISTS `logs` (
                          `user_id` varchar(25) NOT NULL,
                          `month` date NOT NULL,
                          `time` int(11) DEFAULT NULL,
                          `pages` int(11) DEFAULT NULL,
                          PRIMARY KEY (`user_id`)
                        );'''
        cursor.execute(create_logs_table)
        db_conn.commit()
        return True
    except:
        logging.exception('Could not bootstrap database')
        return False
    finally:
        cursor.close()
        db_conn.close()
try:
    with open('creds.json', 'r') as file:
        creds = json.load(file)
except:
    logging.exception('Could not load credentials file')
    sys.exit(1)

bot = commands.Bot(command_prefix='$')
db_pool = create_db_connpool(DB_HOST, creds['mysql']['user'], creds['mysql']['pass'], DB_NAME)
streak = Streaks(db_pool, CMD_COOLDOWN)

@bot.event
async def on_ready():
    logging.info(f'Logged on as {bot.user}!')


@bot.event
async def on_message(message):
    await bot.process_commands(message)
    channel = message.channel
    if channel == 'bot-testing':
        await print('Message from {0.author}: {0.content}'.format(message))


@bot.event
async def on_command_error(ctx, error):
    error = getattr(error, "original", error)
    if isinstance(error, commands.CommandOnCooldown):
        min, sec = divmod(error.retry_after, 60)
        hour, min = divmod(min, 60)
        await ctx.send(f'Try again in {int(hour)} hours, {int(min)} minutes, and {int(sec)} seconds')
    else:
        raise error  # re-raise the error so all the errors will still show up in console

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
            leaderboard_text += f'**{counter}.** {username}  -  {curr_streak}\n'
            counter += 1

        embed = discord.Embed(color=0x00bfff)
        embed.set_thumbnail(url=ctx.guild.icon_url)
        embed.add_field(name='Streak Leaderboard', value=leaderboard_text, inline=True)
        embed.set_footer(text=f'Increase your streak by drawing each day and using {bot.command_prefix}daily!')
        await ctx.send(embed=embed)


    @commands.command(help=f'''Displays the personal best leaderboard for daily streak.
    This leaderboard shows the best unbroken streaks of all time. You can check
    the personal best of an individual user using {bot.command_prefix}pb <user>''',
    brief='Displays personal best leaderboard')
    async def pb(self, ctx, user: discord.Member = None):
        if user is None:
            personal_best = streak.get_pb_leaderboard()
            counter = 1
            pb_leaderboard_text = ''
            for user, pb in personal_best:
                username = self.bot.get_user(int(user))
                pb_leaderboard_text += f'**{counter}.** {username}  -  {pb}\n'
                counter += 1

            embed = discord.Embed(color=0x00bfff)
            embed.set_thumbnail(url=ctx.guild.icon_url)
            embed.add_field(name='Personal Best Leaderboard',
                            value=pb_leaderboard_text, inline=True)
            embed.set_footer(text=f'Set new records by drawing each day and using {bot.command_prefix}daily!')
            await ctx.send(embed=embed)
        else:
            personal_best = streak.get_user_pb(user.id)
            if personal_best is not None:
                personal_best = personal_best[0]
                await ctx.send(f'Current personal best for {user} is {personal_best}')
            else:
                raise commands.BadArgument

    @pb.error
    async def pb_error(self, ctx, error):
        if isinstance(error, commands.BadArgument):
            await ctx.send("Unable to look up personal best for that user.")
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
