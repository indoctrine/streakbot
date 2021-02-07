from datetime import datetime
import discord
from discord.ext import commands
import logging
import aiomysql
import asyncio
import sys

logging.basicConfig(format='%(asctime)s [%(levelname)s] %(message)s',
                    stream=sys.stderr, level=logging.INFO)


class Streak_Commands(commands.Cog, name='Streak Commands'):
    def __init__(self, bot):
        self.bot = bot
        self.reminders = {}

    ####################################
    ####      Commands Section      ####
    ####################################
    @commands.command(help='''Running this command will allow you to add to your
    daily streak for drawing each day. This command is on a 23 hour cooldown so
    if your times start to creep, you can slowly bring them back. 48 hours without
    adding to your daily will reset your streak back to 0 (or 1 upon claim)''',
    brief='Add to your drawing streak')
    # Will throw CommandOnCooldown error if on CD
    async def daily(self, ctx):
        user_exists = await self.check_user_exists(ctx.message.author.id, ctx.message.author)
        if user_exists:
            streak_success = await self.set_streak(ctx.message.author.id)
            if streak_success['status'] is 'success':
                await ctx.send(f"Daily updated for {ctx.message.author} - your current streak is {streak_success['streak']}")
            elif streak_success['status'] is 'timeout':
                await ctx.send(f"More than 48 hours have passed, {ctx.message.author}\'s streak has been set to {streak_success['streak']}")
            elif streak_success['status'] is 'on_cooldown':
                raise commands.CommandOnCooldown(ctx.bot.CMD_COOLDOWN, streak_success['cooldown'])
        else:
            await ctx.send(f'Could not update daily for {ctx.message.author}')

    @daily.error
    async def daily_error(self, ctx, error):
        if isinstance(error, commands.CommandOnCooldown):
            min, sec = divmod(error.retry_after, 60)
            hour, min = divmod(min, 60)
            user_id = ctx.message.author.id
            await ctx.send(f'Try again in {int(hour)} hours, {int(min)} minutes, and {int(sec)} seconds')
            if error.retry_after <= self.bot.REMINDER_THRESHOLD:
                if user_id not in self.reminders:
                    self.reminders[user_id] = error.retry_after
                    await asyncio.sleep(error.retry_after)
                    await ctx.send(f"Hey <@!{user_id}>, it\'s time to claim your daily")
                    self.reminders.pop(user_id)
        else:
            raise error

    @commands.command(help='''Displays the current leaderboard for daily streak. Streaks
    are timed out when this command is run to ensure all information is up to date. This
    command can take the `current` argument to return only this years' leaderboard''',
    brief='Displays current streak leaderboard')
    async def leaderboard(self, ctx, arg = 'overall'):
        if arg.lower() in ['current', 'overall']:
            stats = await self.get_leaderboard(arg)
        else:
            raise commands.errors.BadArgument()

        leaderboard = await self.generate_leaderboard(f'{arg.capitalize()} Streak Leaderboard', stats, 0x00bfff, ctx.guild.icon_url, f'Increase your streak by drawing each day and using {ctx.bot.command_prefix}daily!')
        await ctx.send(embed=leaderboard)

    @leaderboard.error
    async def leaderboard_error(self, ctx, error):
        if isinstance(error, commands.errors.BadArgument):
            await ctx.send(f'Invalid argument for {ctx.bot.command_prefix}leaderboard command.')
        else:
            raise error

    @commands.command(help=f'''Displays the personal best leaderboard for daily streak.
    This leaderboard shows the best unbroken streaks of all time when run with the pb command.
    To look up other years, add the <year> argument.
    You can check the personal best of an individual user using both the <year> and <user> arguments''',
    brief='Displays personal best leaderboard')
    async def pb(self, ctx, year: int = 0, user: discord.Member = None):
        current_year = datetime.now().year
        if year > current_year:
            await ctx.send(f'Year is in the future, please enter a valid year')
            return False
        if user is None:
            personal_best = await self.get_pb_leaderboard(year)
            if personal_best:
                pb_leaderboard = await self.generate_leaderboard(f'{year if year > 0 else "All Time"} Personal Best Leaderboard', personal_best, 0x00bfff, ctx.guild.icon_url, f'Set new records by drawing each day and using {ctx.bot.command_prefix}daily!')
                await ctx.send(embed=pb_leaderboard)
                return True
            else:
                await ctx.send(f'No valid leaderboard for {year}')
                return False
        else:
            personal_best = await self.get_user_pb(user.id, year)
            if personal_best is not None:
                personal_best = personal_best[0]
                await ctx.send(f'Personal best of {year if year > 0 else "all time"} for {user} {"was" if year < current_year else "is"} {personal_best}')
                return True
            else:
                raise commands.BadArgument

    @pb.error
    async def pb_error(self, ctx, error):
        if isinstance(error, commands.BadArgument):
            await ctx.send(f'Invalid arguments \n Usage: `{ctx.bot.command_prefix}pb <year> <user>`')
        else:
            raise error


    ####################################
    #### Logic and Database Section ####
    ####################################

    async def set_streak(self, user_id):
        '''Update the streak of the calling user. Checks the database for the
        last claimed date and compares against current time and the cooldown.
        Also checks if the new year has begun and calls to the rollover logic'''
        curr_time = datetime.now()
        try:
            async with self.bot.db_pool.acquire() as conn:
                async with conn.cursor() as cur:
                    response = {}
                    query = 'SELECT daily_claimed FROM users WHERE user_id = %s'
                    await cur.execute(query, (user_id,))
                    results = await cur.fetchone()
                    last_claimed = results[0]
                    if last_claimed is not None:
                        cd_remaining = curr_time - last_claimed
                        cd_remaining = cd_remaining.total_seconds()
                        if cd_remaining < self.bot.CMD_COOLDOWN:
                            cd_delta = self.bot.CMD_COOLDOWN - cd_remaining
                            response['cooldown'] = cd_delta
                            response['status'] = 'on_cooldown'
                            return response
                        # Reset if cooldown exceeds timeout
                        elif cd_remaining >= self.bot.STREAK_TIMEOUT:
                            query = '''UPDATE users SET daily_claimed = %s, streak = 1
                                    WHERE user_id = %s'''
                            await cur.execute(query, (curr_time, user_id,))
                            await conn.commit()
                            response['status'] = 'timeout'
                            response['streak'] = 1
                            return response
                    # Update the current year and personal best counters
                    query = '''UPDATE users SET daily_claimed = %s,
                            streak = streak + 1, personal_best = CASE WHEN
                            streak + 1 > personal_best THEN streak
                            ELSE personal_best END,
                            current_year_streak = current_year_streak + 1,
                            current_year_best = CASE WHEN
                            current_year_streak + 1 > current_year_best THEN current_year_streak
                            ELSE current_year_best END
                            WHERE user_id = %s'''

                    # Check for new year and rollover
                    is_new_year = await self.compare_db_year()
                    if is_new_year:
                        await self.rollover_streaks(is_new_year)
                        await cur.execute(query, (curr_time, user_id,))
                        await conn.commit()
                    else:
                        # Ensure the history table is correctly updated for year
                        await cur.execute(query, (curr_time, user_id,))
                        await conn.commit()
                        await self.rollover_streaks(is_new_year)
                    response['status'] = 'success'

                    # Grab streak length to return
                    response['streak'] = await self.get_streak(user_id)
                    return response
        except Exception as e:
            logging.exception(f'Could not update streak - {e}')
            return None

    async def compare_db_year(self):
        '''Checks if the current year is greater than DB year'''
        current_year = datetime.now().year
        try:
            async with self.bot.db_pool.acquire() as conn:
                async with conn.cursor() as cur:
                    query = '''SELECT MAX(daily_claimed) FROM users'''
                    await cur.execute(query)
                    results = await cur.fetchone()
                    results = results[0]
                    if results is not None:
                        db_year: datetime = results.year
                        if current_year > db_year:
                            return True
                        else:
                            return False
        except Exception as e:
            logging.exception(f'Database error {e}')
            return None

    async def rollover_streaks(self, is_new_year):
        '''Updates history table and rolls over if a new year has begun'''
        try:
            currdate = datetime.now()
            curr_year = currdate.year

            async with self.bot.db_pool.acquire() as conn:
                async with conn.cursor() as cur:
                    if is_new_year:
                        db_year = curr_year - 1
                    else:
                        db_year = curr_year

                    query = f'''INSERT INTO streak_history (user_id, past_pb, year)
                            SELECT user_id, current_year_best, {db_year} FROM users
                            ON DUPLICATE KEY UPDATE streak_history.past_pb =
                            users.current_year_best'''
                    await cur.execute(query)
                    await conn.commit()

                    if is_new_year:
                        query = '''UPDATE users SET current_year_best = 0, current_year_streak = 0'''
                        await cur.execute(query)
                        await conn.commit()
                    return True
        except Exception as e:
            logging.exception(f'Unable to rollover streaks - {e}')
            return False

    async def get_streak(self, user_id):
        '''Gets the current streak of the calling user'''
        try:
            async with self.bot.db_pool.acquire() as conn:
                async with conn.cursor() as cur:
                    query = 'SELECT streak FROM users WHERE user_id = %s'
                    await cur.execute(query, (user_id,))
                    streak = await cur.fetchone()
                    return streak[0]
        except Exception as e:
            logging.exception(f'Unable to get streak - {e}')
            return False

    async def check_user_exists(self, user_id, fulluser):
        '''Checks if user exists within the database and calls to create_user()
        if user does not'''
        try:
            async with self.bot.db_pool.acquire() as conn:
                async with conn.cursor() as cur:
                    query = 'SELECT * FROM users WHERE user_id = %s'
                    await cur.execute(query, (user_id,))
                    results = await cur.fetchall()
                    if len(results) == 0:
                        user_created = await self.create_user(user_id, fulluser)
                        if user_created:
                            return True
                        else:
                            return False
                    else:
                        return True
        except Exception as e:
            logging.exception(f'Could not check users table - {e}')

    async def create_user(self, user_id, fulluser):
        '''Creates user the first time they try to run the daily command'''
        try:
            fulluser = str(fulluser)
            username = fulluser.split('#')[0]
            discriminator = fulluser.split('#')[1]

            async with self.bot.db_pool.acquire() as conn:
                async with conn.cursor() as cur:
                    query = '''INSERT INTO users (user_id, username, discriminator,
                            streak, personal_best) VALUES (%s, %s, %s, %s, %s)'''
                    await cur.execute(query, (user_id, username, discriminator, 0, 0,))
                    if cur.rowcount > 0:
                        await conn.commit()
                        logging.info(f'User {fulluser} created')
                        return True
                    else:
                        raise Exception('No rows to be written')
        except Exception as e:
            logging.exception(f'Could not create user - {e}')
            return False

    async def timeout_streaks(self):
        '''Times out any users that are outside configurable STREAK_TIMEOUT,
        ensuring that leaderboards are always up to date.'''
        try:
            async with self.bot.db_pool.acquire() as conn:
                async with conn.cursor() as cur:
                    query = '''UPDATE users SET streak = 0 WHERE
                            daily_claimed < NOW() - INTERVAL %s SECOND;'''
                    await cur.execute(query, (self.bot.STREAK_TIMEOUT,))
                    return True
        except Exception as e:
            logging.exception(f'Could not timeout streaks - {e}')
            return False

    async def get_user_pb(self, user_id, year):
        '''Get the user's personal best with optional year parameter'''
        try:
            async with self.bot.db_pool.acquire() as conn:
                async with conn.cursor() as cur:
                    if year == 0:
                        query = 'SELECT personal_best FROM users WHERE user_id = %s'
                        await cur.execute(query, (user_id,))
                    else:
                        query = 'SELECT past_pb FROM streak_history WHERE user_id = %s AND year = %s'
                        await cur.execute(query, (user_id, year,))
            results = await cur.fetchone()
            return results
        except Exception as e:
            logging.exception(f'Unable to get personal best - {e}')
            return False

    async def get_pb_leaderboard(self, year):
        '''Get the personal best leaderboard with optional year parameter'''
        try:
            async with self.bot.db_pool.acquire() as conn:
                async with conn.cursor() as cur:
                    if year == 0:
                        query = '''SELECT user_id, personal_best FROM users ORDER BY
                                personal_best DESC'''
                        await cur.execute(query)
                    else:
                        query = '''SELECT user_id, past_pb FROM streak_history WHERE
                                year = %s ORDER BY past_pb DESC'''
                        await cur.execute(query, (year,))
                    results = await cur.fetchall()
                    return results
        except Exception as e:
            logging.exception(f'Unable to get personal best - {e}')
            return False

    async def get_leaderboard(self, arg):
        '''Get the current year or overall streak leaderboard. This is different
        to the personal best leaderboards as it reflects the ongoing streak'''
        try:
            await self.timeout_streaks()
            async with self.bot.db_pool.acquire() as conn:
                async with conn.cursor() as cur:
                    if arg is 'overall':
                        query = '''SELECT user_id, streak FROM users
                                ORDER BY streak desc'''
                    elif arg.lower() == 'current':
                        query = '''SELECT user_id, current_year_streak FROM
                                users ORDER BY current_year_streak desc'''
                    else:
                        return False
                    await cur.execute(query)
                    results = await cur.fetchall()
                    return results
        except Exception as e:
            logging.exception(f'Unable to get streak - {e}')
            return False

    async def generate_leaderboard(self, title, stats, colour, thumbnail, footer):
        '''Helper function to generate embeds for leaderboards - gracefully handles
        users no longer being on the server.'''
        counter = 1
        leaderboard_text = ''
        for user, stat in stats:
            username = self.bot.get_user(int(user))
            if username is not None:
                leaderboard_text += f'**{counter}.** {username}  -  {stat}\n'
                counter += 1
        embed = discord.Embed(color=colour)
        embed.set_thumbnail(url=thumbnail)
        embed.add_field(name=title, value=leaderboard_text, inline=True)
        embed.set_footer(text=footer)
        return embed

def setup(bot):
    bot.add_cog(Streak_Commands(bot))
