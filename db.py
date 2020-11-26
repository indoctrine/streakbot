import mariadb
from mariadb import Error
import logging
import sys

class Database:
    def __init__(self, host_name, user_name, user_password, db):
        self.conn_pool = None
        self.host_name = host_name
        self.user_name = user_name
        self.user_password = user_password
        self.db = db

        try:
            if self.bootstrap_db(user_name, user_password, host_name, db):
                self.conn_pool = mariadb.ConnectionPool(
                    pool_name='streakbot_pool',
                    pool_size=5,
                    pool_reset_connection=True,
                    host=host_name,
                    user=user_name,
                    password=user_password,
                    database=db)
                logging.info('Connection to MySQL DB successful')
            else:
                raise Exception('Cannot bootstrap DB')
        except Error as e:
            logging.exception(f'Error while connecting to MySQL using Connection pool {e}')
            sys.exit(1)

    def refresh_db_pool(self):
        try:
            db_conn = self.conn_pool.get_connection()
            db_conn.close()
            return True
        except mariadb.PoolError as e:
            for x in range(5):
                tempconn = mariadb.connect(
                    user=self.user_name,
                    password=self.user_password,
                    host=self.host_name,
                    database=self.db)
                self.conn_pool.add_connection(tempconn)
            logging.info('Connection pool refreshed')
            return True
        except Error as e:
            logging.exception(f'Error while connecting to MySQL using Connection pool {e}')
            return False

    def bootstrap_db(self, user_name, user_password, host_name, db):
        try:
            db_conn = mariadb.connection(
                        user=user_name,
                        password=user_password,
                        host=host_name)
            cursor = db_conn.cursor()
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
                              `month` date NOT NULL,
                              `time` int(11) DEFAULT NULL,
                              `pages` int(11) DEFAULT NULL,
                              PRIMARY KEY (`user_id`)
                            );'''
            create_streak_history = '''CREATE TABLE IF NOT EXISTS `streak_history`
                                    (
                                    `user_id` VARCHAR(50) NOT NULL,
                                    `year` YEAR NOT NULL,
                                    `past_pb` INT NOT NULL,
                                    CONSTRAINT id PRIMARY KEY (user_id,year)
                                    )'''

            # Create database
            cursor.execute(create_db.format(db=db))
            db_conn.commit()

            # Move cursor to database
            cursor.execute(use_db.format(db=db))

            # Create the tables
            cursor.execute(create_user_table)
            cursor.execute(create_logs_table)
            cursor.execute(create_streak_history)
            db_conn.commit()
            return True
        except:
            logging.exception('Could not bootstrap database')
            return False
        finally:
            cursor.close()
            db_conn.close()
