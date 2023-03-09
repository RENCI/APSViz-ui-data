# SPDX-FileCopyrightText: 2022 Renaissance Computing Institute. All rights reserved.
# SPDX-FileCopyrightText: 2023 Renaissance Computing Institute. All rights reserved.
#
# SPDX-License-Identifier: GPL-3.0-or-later
# SPDX-License-Identifier: LicenseRef-RENCI
# SPDX-License-Identifier: MIT

"""
    Base class for database functionalities

    Author: Phil Owen, RENCI.org
"""

import os
import time
from collections import namedtuple
import psycopg2

from src.common.logger import LoggingUtil


class PGUtilsMultiConnect:
    """
        Base class for database functionalities.

        This class supports setting up connections to multiple databases. To do that
        the class relies on environment parameter names that adhere to a specific
        naming convention. e.g. <DB name>_DB_<parameter name>. Note that the
        final environment parameter should be all uppercase.

        Please see the get_conn_config() method below for more details.
    """

    def __init__(self, app_name, db_names: tuple):
        """
        Entry point for the db connection creation and operations

        :param db_names:
        """
        # get the log level and directory from the environment.
        log_level, log_path = LoggingUtil.prep_for_logging()

        # create a logger
        self.logger = LoggingUtil.init_logging(f"{app_name}.PGUtilsMultiConnect", level=log_level, line_format='medium', log_file_path=log_path)

        # create a dict for the DB connection details
        self.dbs: dict = {}

        # create the named tuple definition for DB info
        self.db_info_tpl: namedtuple = namedtuple('DB_Info', ['name', 'conn_str', 'conn', 'cursor'])

        # save the DB names for connection/cursor closing on class tear-down
        self.db_names: tuple = db_names

        # get the details loaded into a tuple for all the DBs
        for db_name in self.db_names:
            # get the connection string
            conn_config = self.get_conn_config(db_name)

            # create a temporary tuple to get the discovery process started
            temp_tuple: namedtuple = self.db_info_tpl(db_name, conn_config, None, None)

            # save the verified db connection info
            db_info: namedtuple = self.get_db_connection(temp_tuple)

            # add the verified connection to the dict
            self.dbs.update({db_name: db_info})

    def __del__(self):
        """
        Close up the DB connections and cursors

        :return:
        """
        # for each db name specified
        for db_name in self.db_names:
            try:
                # in there is a cursor, close it
                if self.dbs[db_name].cursor is not None:
                    # get the item out of the tuple
                    cursor = self.dbs[db_name].cursor

                    # close it
                    cursor.close()

                # if there is a connection, close it
                if self.dbs[db_name].conn is not None:
                    # get the item out of the tuple
                    conn = self.dbs[db_name].conn

                    # close it
                    conn.close()
            except Exception:
                self.logger.exception('Error detected closing the cursor or connection for %s.', db_name)

    @staticmethod
    def get_conn_config(db_name: str) -> str:
        """
        Creates a dict of the DB connection configuration.

        :param db_name:
        :return:
        """
        # insure the env parameter prefix is uppercase
        db_name: str = db_name.upper()

        # get configuration params from the env params
        user: str = os.environ.get(f'{db_name}_DB_USERNAME')
        password: str = os.environ.get(f'{db_name}_DB_PASSWORD')
        dbname: str = os.environ.get(f'{db_name}_DB_DATABASE')
        host: str = os.environ.get(f'{db_name}_DB_HOST')
        port: int = int(os.environ.get(f'{db_name}_DB_PORT'))

        # create a connection string
        connection_str: str = f"host={host} port={port} dbname={dbname} user={user} password={password}"

        # return to the caller
        return connection_str

    def get_db_connection(self, db_info: namedtuple) -> object:
        """
        Gets a connection to the DB. performs a check to continue trying until
        a connection is made.

        :return:
        """
        # init the connection status indicator
        good_conn: bool = False

        # until forever
        while not good_conn:
            # check the DB connection
            good_conn = self.check_db_connection(db_info)

            try:
                # do we have a good connection
                if not good_conn:
                    # connect to the DB
                    conn = psycopg2.connect(db_info.conn_str)

                    # insure records are updated immediately
                    conn.autocommit = True

                    # create a new db info tuple
                    verified_tuple: namedtuple = self.db_info_tpl(db_info.name, db_info.conn_str, conn, conn.cursor())

                    # check the DB connection
                    good_conn = self.check_db_connection(verified_tuple)

                    # is the connection ok now?
                    if good_conn:
                        self.logger.info('DB Connection established to %s.', db_info.name)

                        # return the verified db info tuple
                        return verified_tuple
                else:
                    # the db info sent is ok to use
                    return db_info
            except Exception:
                self.logger.exception('Error getting connection %s.', db_info.name)
                good_conn = False

            self.logger.error('DB Connection failed to %s. Retrying...', db_info.name)
            time.sleep(5)

    def check_db_connection(self, db_info: namedtuple) -> bool:
        """
        Checks to see if there is a good connection to the DB.

        :param db_info:
        :return: boolean
        """
        # init the return value
        ret_val = None

        try:
            # is there a connection
            if not db_info.conn:
                ret_val = False
            else:
                # get the DB version
                db_info.cursor.execute("SELECT version()")

                # get the value
                db_version = db_info.cursor.fetchone()

                # did we get a value
                if db_version:
                    # update the return flag
                    ret_val = True

        except (Exception, psycopg2.DatabaseError):
            self.logger.exception('Error checking DB connection')

            # connection failed
            ret_val = False

        # return to the caller
        return ret_val

    def exec_sql(self, db_name: str, sql_stmt: str):
        """
        Executes a sql statement.

        :param db_name:
        :param sql_stmt:
        :return:
        """
        # init the return
        ret_val = None

        # get the appropriate db info object
        db_info = self.dbs[db_name]

        # insure we have a valid DB connection
        self.get_db_connection(db_info)

        try:
            # execute the sql
            db_info.cursor.execute(sql_stmt)

            # get the returned value
            ret_val = db_info.cursor.fetchone()

            # trap the return
            if ret_val is None or ret_val[0] is None:
                # specify a return code on an empty result
                ret_val = -1
            else:
                # get the one and only record of json
                ret_val = ret_val[0]

        except Exception:
            self.logger.exception("Error detected executing SQL: %s.", sql_stmt)

            # set the error code
            ret_val = -1

        # return to the caller
        return ret_val
