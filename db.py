import logging
import sys
import aiomysql
import asyncio

class Database:
    def __init__(self, host_name, user_name, user_password, db, pool_size):
        self.conn_pool = None
        self.host_name = host_name
        self.user_name = user_name
        self.user_password = user_password
        self.db = db
        self.pool_size = pool_size

    async def create_pool(self):
        try:
            self.conn_pool = await aiomysql.create_pool(host=self.host_name, user=self.user_name,
                                                    password=self.user_password, db=self.db,
                                                    maxsize=self.pool_size)
            logging.info('Connection to MySQL DB successful')
            return self.conn_pool
        except Error as e:
            logging.exception(f'Error while creating MySQL Connection Pool {e}')
            sys.exit(1)

    async def bootstrap_db(self):
        try:
            db_conn = await aiomysql.connect(host=self.host_name, user=self.user_name,
                                                  password=self.user_password)
            cursor = await db_conn.cursor()
            create_db = 'CREATE DATABASE IF NOT EXISTS {db}'
            use_db = 'USE {db}'
            create_user_table = '''CREATE TABLE IF NOT EXISTS `users` (
                            	`user_id` VARCHAR(50) NOT NULL,
                            	`username` VARCHAR(50) NOT NULL,
                            	`discriminator` INT NOT NULL,
                            	`daily_claimed` DATETIME,
                            	`streak` INT DEFAULT 0,
                            	`personal_best` INT DEFAULT 0,
                                `current_year_best` INT DEFAULT 0,
                                `current_year_streak` INT DEFAULT 0,
                            	PRIMARY KEY (`user_id`)
                            );'''
            create_logs_table = '''CREATE TABLE IF NOT EXISTS `logs` (
                              `user_id` varchar(50) NOT NULL,
                              `month` varchar(8) NOT NULL,
                              `time` int(11) DEFAULT 0,
                              `pages` int(11) DEFAULT 0,
                              CONSTRAINT PRIMARY KEY (user_id,month)
                            );'''
            create_streak_history = '''CREATE TABLE IF NOT EXISTS `streak_history`
                                    (
                                    `user_id` VARCHAR(50) NOT NULL,
                                    `year` YEAR NOT NULL,
                                    `past_pb` INT NOT NULL,
                                    CONSTRAINT PRIMARY KEY (user_id,year)
                                    )'''

            # Create database
            await cursor.execute(create_db.format(db=self.db))

            # Move cursor to database
            await cursor.execute(use_db.format(db=self.db))

            # Create the tables
            await cursor.execute(create_user_table)
            await cursor.execute(create_logs_table)
            await cursor.execute(create_streak_history)
            return True
        except:
            logging.exception('Could not bootstrap database')
            sys.exit(1)
        finally:
            db_conn.close()
