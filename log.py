from datetime import datetime
import logging
import aiomysql
import asyncio
import sys

class Log:
    def __init__(self, db_pool):
        self.db_pool = db_pool
    async def log_pages(self):
        curr_datetime = datetime.now()
        curr_month = curr_datetime.strftime('%m-%Y')
        print(curr_month)
