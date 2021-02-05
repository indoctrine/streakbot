from datetime import datetime
import logging
import aiomysql
import asyncio
import sys

logging.basicConfig(format='%(asctime)s [%(levelname)s] %(message)s',
                    stream=sys.stderr, level=logging.INFO)

class Streaks:
    def __init__(self, db_pool, cmd_cd, timeout):
        self.db_pool = db_pool
        self.cooldown = cmd_cd
        self.timeout = timeout

    async def check_user_exists(self, user_id, fulluser):
        try:
            async with self.db_pool.conn_pool.acquire() as conn:
                async with conn.cursor() as cur:
                    query = 'SELECT * FROM users WHERE user_id = %s'
                    await cur.execute(query, (user_id,))
                    results = await cur.fetchall()
                    if len(results) == 0:
                        user_created = await self.create_user(user_id, fulluser, conn)
                        if user_created:
                            return True
                        else:
                            return False
                    else:
                        return True
        except Exception as e:
            logging.exception(f'Could not check users table - {e}')

    async def create_user(self, user_id, fulluser, db_conn):
        try:
            fulluser = str(fulluser)
            username = fulluser.split('#')[0]
            discriminator = fulluser.split('#')[1]

            async with self.db_pool.conn_pool.acquire() as conn:
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

    async def get_streak(self, user_id):
        try:
            async with self.db_pool.conn_pool.acquire() as conn:
                async with conn.cursor() as cur:
                    query = 'SELECT streak FROM users WHERE user_id = %s'
                    await cur.execute(query, (user_id,))
                    streak = await cur.fetchone()
                    return streak[0]
        except Exception as e:
            logging.exception(f'Unable to get streak - {e}')
            return False

    async def get_leaderboard(self, arg):
        try:
            await self.timeout_streaks()
            async with self.db_pool.conn_pool.acquire() as conn:
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

    async def timeout_streaks(self):
        try:
            async with self.db_pool.conn_pool.acquire() as conn:
                async with conn.cursor() as cur:
                    query = '''UPDATE users SET streak = 0 WHERE
                            daily_claimed < NOW() - INTERVAL %s SECOND;'''
                    await cur.execute(query, (self.timeout,))
                    return True
        except Exception as e:
            logging.exception(f'Could not timeout streaks - {e}')
            return False

    async def set_streak(self, user_id):
        curr_time = datetime.now()
        try:
            async with self.db_pool.conn_pool.acquire() as conn:
                async with conn.cursor() as cur:
                    response = {}
                    query = 'SELECT daily_claimed FROM users WHERE user_id = %s'
                    await cur.execute(query, (user_id,))
                    results = await cur.fetchone()
                    last_claimed = results[0]
                    if last_claimed is not None:
                        cd_remaining = curr_time - last_claimed
                        cd_remaining = cd_remaining.total_seconds()
                        if cd_remaining < self.cooldown:
                            cd_delta = self.cooldown - cd_remaining
                            response['cooldown'] = cd_delta
                            response['status'] = 'on_cooldown'
                            return response
                        # Reset if cooldown exceeds timeout
                        elif cd_remaining >= self.timeout:
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
        # Checks if current year is less than DB year
        current_year = datetime.now().year
        try:
            async with self.db_pool.conn_pool.acquire() as conn:
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

    async def get_user_pb(self, user_id, year):
        try:
            async with self.db_pool.conn_pool.acquire() as conn:
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
        try:
            async with self.db_pool.conn_pool.acquire() as conn:
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

    async def rollover_streaks(self, is_new_year):
        try:
            currdate = datetime.now()
            curr_year = currdate.year

            async with self.db_pool.conn_pool.acquire() as conn:
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
