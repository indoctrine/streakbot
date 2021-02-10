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
        if log_type not in log_types:
            raise commands.errors.BadArgument()
        else:
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

def setup(bot):
    bot.add_cog(Log_Commands(bot))
