from discord.ext import commands
from datetime import datetime
import logging
import mariadb
import sys

logging.basicConfig(format='%(asctime)s [%(levelname)s] %(message)s',
                    stream=sys.stderr, level=logging.INFO)


class Streaks:
    def __init__(self, db_pool, cmd_cd):
        self.conn_pool = db_pool
        self.cooldown = cmd_cd

    def check_user_exists(self, user_id, fulluser):
        try:
            db_conn = self.conn_pool.get_connection()
            cursor = db_conn.cursor()
            query = 'SELECT * FROM users WHERE user_id = %s'
            cursor.execute(query, (user_id,))
            results = cursor.fetchall()
            if len(results) == 0:
                if self.create_user(user_id, fulluser, db_conn):
                    return True
                else:
                    return False
            else:
                return True
        except mariadb.Error as e:
            logging.exception(f'Could not check users table - {e}')
        finally:
            cursor.close()
            db_conn.close()

    def create_user(self, user_id, fulluser, db_conn):
        try:
            fulluser = str(fulluser)
            username = fulluser.split('#')[0]
            discriminator = fulluser.split('#')[1]

            cursor = db_conn.cursor()
            query = '''INSERT INTO users (user_id, username, discriminator,
                        streak, personal_best) VALUES (%s, %s, %s, %s, %s)'''
            cursor.execute(query, (user_id, username, discriminator, 0, 0,))

            if cursor.rowcount > 0:
                db_conn.commit()
                logging.info(f'User {fulluser} created')
                return True
            else:
                raise Exception('No rows to be written')
        except mariadb.Error as e:
            logging.exception(f'Could not create user - {e}')
            return False
        finally:
            cursor.close()

    def get_streak(self, user_id):
        try:
            db_conn = self.conn_pool.get_connection()
            cursor = db_conn.cursor()
            query = 'SELECT streak FROM users WHERE user_id = %s'
            cursor.execute(query, (user_id,))
            streak = cursor.fetchone()
            return streak[0]
        except mariadb.Error as e:
            logging.exception(f'Unable to get streak - {e}')
            return False
        finally:
            cursor.close()
            db_conn.close()

    def get_leaderboard(self):
        try:
            db_conn = self.conn_pool.get_connection()
            cursor = db_conn.cursor()
            query = '''SELECT user_id, streak FROM users
                    ORDER BY streak desc'''
            cursor.execute(query)
            results = cursor.fetchall()
            return results
        except mariadb.Error as e:
            logging.exception(f'Unable to get streak - {e}')
            return False
        finally:
            cursor.close()
            db_conn.close()

    def timeout_streaks(self, timeout_duration=172800):
        try:
            db_conn = self.conn_pool.get_connection()
            cursor = db_conn.cursor()
            query = '''UPDATE users SET streak = 0 WHERE
                    daily_claimed < NOW() - INTERVAL %s SECOND;'''
            cursor.execute(query, (timeout_duration,))
            db_conn.commit()
        except mariadb.Error as e:
            logging.exception(f'Could not timeout streaks - {e}')
            return None
        finally:
            cursor.close()
            db_conn.close()

    def set_streak(self, user_id, timeout_duration=172800):
        curr_time = datetime.now()
        try:
            db_conn = self.conn_pool.get_connection()
            cursor = db_conn.cursor()
            query = 'SELECT daily_claimed FROM users WHERE user_id = %s'
            cursor.execute(query, (user_id,))
            results = cursor.fetchone()
            last_claimed = results[0]
            if last_claimed is not None:
                cd_remaining = curr_time - last_claimed
                cd_remaining = cd_remaining.total_seconds()
                if cd_remaining <= self.cooldown:
                    cd_delta = self.cooldown - cd_remaining
                    # Send cooldown message to user
                    raise commands.CommandOnCooldown(self.cooldown, cd_delta)
                    return None
                # Reset if cooldown exceeds timeout
                elif cd_remaining >= timeout_duration:
                    query = '''UPDATE users SET daily_claimed = %s, streak = 1
                            WHERE user_id = %s'''
                    cursor.execute(query, (curr_time, user_id,))
                    db_conn.commit()
                    return False
            # Update the current year and personal best counters
            query = '''UPDATE users SET daily_claimed = %s,
                    streak = streak + 1, personal_best = CASE WHEN
                    streak + 1 > personal_best THEN streak
                    ELSE personal_best END,
                    current_year_best = CASE WHEN
                    streak + 1 > current_year_best THEN streak
                    ELSE current_year_best END,
                    current_year_streak = current_year_streak + 1
                    WHERE user_id = %s'''

            # Check for new year and rollover
            is_new_year = self.compare_db_year()
            if is_new_year:
                self.rollover_streaks(is_new_year)
                cursor.execute(query, (curr_time, user_id,))
            else:
                # Ensure the history table is correctly updated for year
                cursor.execute(query, (curr_time, user_id,))
                self.rollover_streaks(is_new_year)
            db_conn.commit()
            return True
        except mariadb.Error as e:
            logging.exception(f'Could not update streak - {e}')
            return None
        finally:
            cursor.close()
            db_conn.close()

    def compare_db_year(self):
        # Checks if current year is less than DB year
        current_year = datetime.now().year
        try:
            db_conn = self.conn_pool.get_connection()
            cursor = db_conn.cursor()
            query = '''SELECT MAX(daily_claimed) FROM users'''
            cursor.execute(query)
            results = cursor.fetchone()[0]
            if results is not None:
                db_year: datetime = results.year
                if current_year > db_year:
                    return True
                else:
                    return False
        except mariadb.Error as e:
            logging.exception(f'Database error {e}')
            return None
        finally:
            cursor.close()
            db_conn.close()

    def get_user_pb(self, user_id, year):
        try:
            db_conn = self.conn_pool.get_connection()
            cursor = db_conn.cursor()
            if year == 0:
                query = 'SELECT personal_best FROM users WHERE user_id = %s'
                cursor.execute(query, (user_id,))
            else:
                query = 'SELECT past_pb FROM streak_history WHERE user_id = %s AND year = %s'
                cursor.execute(query, (user_id, year,))
            results = cursor.fetchone()
            return results
        except mariadb.Error as e:
            logging.exception(f'Unable to get personal best - {e}')
            return False
        finally:
            cursor.close()
            db_conn.close()

    def get_pb_leaderboard(self, year):
        try:
            db_conn = self.conn_pool.get_connection()
            cursor = db_conn.cursor()
            if year == 0:
                query = '''SELECT user_id, personal_best FROM users ORDER BY
                        personal_best DESC'''
                cursor.execute(query)
            else:
                query = '''SELECT user_id, past_pb FROM streak_history WHERE
                        year = %s ORDER BY past_pb DESC'''
                cursor.execute(query, (year,))
            results = cursor.fetchall()
            return results
        except mariadb.Error as e:
            logging.exception(f'Unable to get personal best - {e}')
            return False
        finally:
            cursor.close()
            db_conn.close()

    def rollover_streaks(self, is_new_year):
        try:
            currdate = datetime.now()
            curr_year = currdate.year
            db_conn = self.conn_pool.get_connection()
            cursor = db_conn.cursor()
            if is_new_year:
                db_year = curr_year - 1
            else:
                db_year = curr_year
            query = f'''INSERT INTO streak_history (user_id, past_pb, year)
                    SELECT user_id, current_year_best, {db_year} FROM users
                    ON DUPLICATE KEY UPDATE streak_history.past_pb =
                    users.current_year_best'''
            cursor.execute(query)
            if db_year > curr_year:
                query = '''UPDATE users SET current_year_best = 0, current_year_streak = 0'''
                cursor.execute(query)
            return True
        except mariadb.Error as e:
            logging.exception(f'Unable to rollover streaks - {e}')
            return False
        finally:
            cursor.close()
            db_conn.close()
