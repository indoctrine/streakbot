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

    @commands.command(help='''This command will allow you to log time or pages
    drawn.''', brief='Log time or pages for this month')
    async def log(self, ctx, log_type, amount: int = 0):
        log_type = log_type.lower()
        log_types = ['pages', 'page', 'time']
        if log_type in log_types:
            user_exists = await self.check_user_exists(ctx.message.author.id, ctx.message.author)
        else:
            raise commands.errors.BadArgument()
        if user_exists:
            if amount == 0:
                raise commands.errors.BadArgument()
            if re.findall('pages?', log_type):
                page_results = await self.log_pages(ctx.message.author.id, amount)
                await ctx.send(f'Added {amount} pages for {ctx.message.author}. Current month total is {page_results}.')
            else:
                print('To be filled with time logging')

    @log.error
    async def log_error(self, ctx, error):
        if isinstance(error, commands.errors.MissingRequiredArgument):
            await ctx.send('Log type is a required argument - valid logging types are `page(s)` or `time`')
        elif isinstance(error, commands.errors.BadArgument):
            await ctx.send('Invalid argument(s), command format is `log {type} {amount}`')
        else:
            raise error

    async def log_pages(self, user_id, pages):
        curr_datetime = datetime.now()
        curr_month = curr_datetime.strftime('%m-%Y')
        query = '''INSERT INTO logs (user_id, month, pages)
                VALUES (%s, %s, %s) ON DUPLICATE KEY UPDATE pages = pages + %s'''
        try:
            async with self.bot.db_pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(query, (user_id, curr_month, pages, pages,))
                    await conn.commit()
                    current_pages = await self.get_user_pages(user_id, curr_month)
                    return current_pages[0]
        except Exception as e:
            logging.exception(f'Could not update page logs - {e}')
            return False

    async def get_user_pages(self, user_id, month):
        query = 'SELECT pages from logs WHERE user_id = %s and month = %s'
        try:
            async with self.bot.db_pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await conn.commit()
                    await cur.execute(query, (user_id, month,))
                    results = await cur.fetchone()
                    return results
        except Exception as e:
            logging.exception(f'Could not get page logs for user {user_id} - {e}')

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

def setup(bot):
    bot.add_cog(Log_Commands(bot))
