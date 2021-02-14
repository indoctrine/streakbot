from datetime import datetime
import discord
from discord.ext import commands
import logging
import aiomysql
import asyncio
import sys
import re

class Log_Commands(commands.Cog, name='Log Commands'):
    def __init__(self, bot):
        self.bot = bot
        self.log_types = ['pages', 'time']


    ####################################
    ####      Commands Section      ####
    ####################################

    @commands.command(help='''This command will allow you to log time or pages
    drawn.''', brief='Log time or pages for this month')
    async def log(self, ctx, log_type, amount: int):
        log_type = log_type.lower()
        if log_type not in self.log_types:
            raise commands.errors.BadArgument('log')
        else:
            if amount == 0:
                raise commands.errors.BadArgument()
            else:
                current_amount = await self.set_log(ctx.message.author.id, log_type, amount)
                if current_amount == False:
                    await ctx.send(f'Unable to update logs for user {ctx.message.author}')
                else:
                    await ctx.send(f'Added {amount} {"minutes" if log_type == "time" else log_type} for {ctx.message.author}. Current month total is {current_amount}.')

    @log.error
    async def log_error(self, ctx, error):
        if isinstance(error, commands.errors.MissingRequiredArgument):
            await ctx.send('Log type and amount are required arguments.')
        elif isinstance(error, commands.errors.BadArgument):
            if str(error) == 'log':
                await ctx.send('Invalid argument(s), command format is `log {type} {amount}` - valid log types are `pages` and `time`')
            else:
                await ctx.send('Invalid argument(s), command format is `log {type} {amount}`, where amount is an integer greater than 0')
        else:
            raise error

    @commands.command(help='''This command will allow you to get a current or
    historical leaderboard for logs.''', brief='Get a leaderboard output for logs')
    async def logboard(self, ctx, log_type, month = None):
        curr_month = datetime.now()
        if log_type not in self.log_types:
            raise commands.errors.BadArgument('log')
        if month == None:
            month = curr_month.strftime('%m-%Y')
        elif not re.match('\d{2}-\d{4}', month):
            raise commands.errors.BadArgument('month_format')
        logboard = await self.get_logboard(log_type, month)
        if logboard:
            embed = await ctx.bot.generate_leaderboard(f'{month} {log_type.capitalize()} Logboard', logboard, 0x00bfff, ctx.guild.icon_url, f'Log your time or pages using {ctx.bot.command_prefix}log!')
            await ctx.send(embed=embed)
        else:
            await ctx.send(f'Unable to fetch {month} {log_type} logboard')
            return False

    @logboard.error
    async def logboard_error(self, ctx, error):
        if isinstance(error, commands.errors.MissingRequiredArgument):
            await ctx.send('Log type is a required arguments.')
        elif isinstance(error, commands.errors.BadArgument):
            if str(error) == 'log':
                await ctx.send(f'Invalid log type, valid log types are `pages` and `time`. See `{ctx.bot.command_prefix}help logboard` for more info.')
            elif str(error) == 'month_format':
                await ctx.send(f'Invalid date format, valid format is MM-YYYY. See `{ctx.bot.command_prefix}help logboard` for more info.')
        else:
            raise error

    @commands.command(help='''This command will allow you to get a current or
    historical log for a user.''', brief='Get user logs')
    async def logbook(self, ctx, log_type, user: discord.Member, month = None):
        curr_month = datetime.now()
        if log_type not in self.log_types:
            raise commands.errors.BadArgument('log')
        if month == None:
            month = curr_month.strftime('%m-%Y')
        elif not re.match('\d{2}-\d{4}', month):
            raise commands.errors.BadArgument('month_format')

        user_log = await self.get_user_logs(user.id, month, log_type)
        if user_log is False:
            await ctx.send(f'Unable to retrieve {log_type} stats for {user} for {month}')
            return False
        if log_type == 'time':
            if user_log > 60:
                hours, mins = divmod(user_log, 60)
                user_log = f'{hours} hours, {mins} minutes'
            else:
                user_log = f'{user_log} minutes'
        if curr_month.strftime('%m-%Y') == month:
            await ctx.send(f'Current month {log_type} logging stats for {user}: {user_log}')
        else:
            await ctx.send(f'{month} {log_type} logging stats for {user}: {user_log}')

    @logbook.error
    async def logbook_error(self, ctx, error):
        if isinstance(error, commands.errors.MissingRequiredArgument):
            await ctx.send('Log type and user are required arguments.')
        elif isinstance(error, commands.errors.BadArgument):
            if str(error) == 'log':
                await ctx.send(f'Invalid log type, valid log types are `pages` and `time`. See `{ctx.bot.command_prefix}help logbook` for more info.')
            elif str(error) == 'month_format':
                await ctx.send(f'Invalid date format, valid format is MM-YYYY. See `{ctx.bot.command_prefix}help logbook` for more info.')
        else:
            raise error


    ####################################
    #### Logic and Database Section ####
    ####################################

    async def get_logboard(self, log_type, month):
        '''Get the specified month logboard.'''
        try:
            async with self.bot.db_pool.acquire() as conn:
                async with conn.cursor() as cur:
                    query = f'''SELECT user_id, {log_type} from logs
                            WHERE month = %s ORDER BY {log_type} DESC'''
                    await cur.execute(query, (month,))
                    results = await cur.fetchall()
                    result_dict = {}
                    for result in results:
                        hours, mins = divmod(result[1], 60)
                        if hours > 0:
                            result_dict[result[0]] = f'{hours} hours, {mins} minutes'
                        else:
                            result_dict[result[0]] = f'{result[1]} minutes'
                    return results
        except Exception as e:
            logging.exception(f'Unable to get logboard - {e}')
            return False

    async def set_log(self, user_id, type, amount):
        curr_datetime = datetime.now()
        curr_month = curr_datetime.strftime('%m-%Y')
        query = f'''INSERT INTO logs (user_id, month, {type})
                VALUES (%s, %s, %s) ON DUPLICATE KEY UPDATE {type} = {type} + %s'''
        try:
            async with self.bot.db_pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(query, (user_id, curr_month, amount, amount,))
                    await conn.commit()
                    current_logs = await self.get_user_logs(user_id, curr_month, type)
                    if type == 'time' and current_logs:
                        hours, mins = divmod(current_logs, 60)
                        if hours > 0:
                            return f'{hours} hours, {mins} minutes'
                        else:
                            return f'{current_logs} minutes'
                    elif current_logs:
                        return f'{current_logs} pages'
                    else:
                        raise Exception('No logs retrievable for users')
        except Exception as e:
            logging.exception(f'Could not update page logs - {e}')
            return False

    async def get_user_logs(self, user_id, month, type):
        query = f'SELECT {type} from logs WHERE user_id = %s and month = %s'
        try:
            async with self.bot.db_pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await conn.commit()
                    await cur.execute(query, (user_id, month,))
                    results = await cur.fetchone()
                    if results is None:
                        return False
                    else:
                        return results[0]
        except Exception as e:
            logging.exception(f'Could not get page logs for user {user_id} - {e}')

def setup(bot):
    bot.add_cog(Log_Commands(bot))
