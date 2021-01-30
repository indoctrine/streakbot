from datetime import datetime
import logging
import aiomysql
import asyncio
import sys

class Log:
    def __init__(self, db_pool):
        self.db_pool = db_pool
    async def log_pages(self, user_id, pages):
        curr_datetime = datetime.now()
        curr_month = curr_datetime.strftime('%m-%Y')
        query = '''INSERT INTO logs (user_id, month, pages)
                VALUES (%s, %s, %s) ON DUPLICATE KEY UPDATE pages = pages + %s'''
        try:
            async with self.db_pool.conn_pool.acquire() as conn:
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
            async with self.db_pool.conn_pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await conn.commit()
                    await cur.execute(query, (user_id, month,))
                    results = await cur.fetchone()
                    return results
        except Exception as e:
            logging.exception(f'Could not get page logs for user {user_id} - {e}')
