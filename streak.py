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

    def create_user(self, user_id, fulluser, db_conn):
        try:
            cursor = db_conn.cursor()
            query = '''INSERT INTO users (user_id, username, discriminator,
                        streak, personal_best) VALUES (%s, %s, %s, %s, %s)'''
            fulluser = str(fulluser)
            username = fulluser.split('#')[0]
            discriminator = fulluser.split('#')[1]
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
                    ORDER BY streak desc LIMIT 10'''
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
                    TIME_TO_SEC(TIMEDIFF(NOW(), daily_claimed)) >= %s'''
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
                    raise commands.CommandOnCooldown(self.cooldown, cd_delta)
                    return None
                # 2 days without claiming
                elif cd_remaining >= timeout_duration:
                    query = '''UPDATE users SET daily_claimed = %s, streak = 1
                            WHERE user_id = %s'''
                    cursor.execute(query, (curr_time, user_id,))
                    db_conn.commit()
                    return False
            query = '''UPDATE users SET daily_claimed = %s,
                    streak = streak + 1, personal_best = CASE WHEN
                    streak + 1 > personal_best THEN streak
                    ELSE personal_best END WHERE user_id = %s'''
            cursor.execute(query, (curr_time, user_id,))
            db_conn.commit()
            return True
        except mariadb.Error as e:
            logging.exception(f'Could not update streak - {e}')
            return None
        finally:
            cursor.close()
            db_conn.close()

    def get_user_pb(self, user_id):
        try:
            db_conn = self.conn_pool.get_connection()
            cursor = db_conn.cursor()
            query = 'SELECT personal_best FROM users WHERE user_id = %s'
            cursor.execute(query, (user_id,))
            results = cursor.fetchone()
            return results
        except mariadb.Error as e:
            logging.exception(f'Unable to get personal best - {e}')
            return False
        finally:
            cursor.close()
            db_conn.close()

    def get_pb_leaderboard(self):
        try:
            db_conn = self.conn_pool.get_connection()
            cursor = db_conn.cursor()
            query = '''SELECT user_id, personal_best FROM users
                     ORDER BY personal_best desc LIMIT 10'''
            cursor.execute(query)
            results = cursor.fetchall()
            return results
        except mariadb.Error as e:
            logging.exception(f'Unable to get personal best - {e}')
            return False
        finally:
            cursor.close()
            db_conn.close()
