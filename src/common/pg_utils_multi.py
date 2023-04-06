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

    def __init__(self, app_name, db_names: tuple, _logger=None, _auto_commit=True):
        """
        Entry point for the db connection creation and operations

        :param db_names:
        """
        # if a reference to a logger passed in use it
        if _logger is not None:
            # get a handle to a logger
            self.logger = _logger
        else:
            # get the log level and directory from the environment.
            log_level, log_path = LoggingUtil.prep_for_logging()

            # create a logger
            self.logger = LoggingUtil.init_logging(f"{app_name}.PGUtilsMultiConnect", level=log_level, line_format='medium', log_file_path=log_path)

        # create a dict for the DB connection details
        self.dbs: dict = {}

        # set the autocommit
        self.auto_commit = _auto_commit

        # create the named tuple definition for DB info
        self.db_info_tpl: namedtuple = namedtuple('DB_Info', ['name', 'conn_str', 'conn'])

        # save the DB names for connection/cursor closing on class tear-down
        self.db_names: tuple = db_names

        # get the details loaded into a tuple for all the DBs
        for db_name in self.db_names:
            # get the connection string
            conn_config = self.get_conn_config(db_name)

            # create a temporary tuple to get the discovery process started
            temp_tuple: namedtuple = self.db_info_tpl(db_name, conn_config, None)

            # get the connection
            self.get_db_connection(temp_tuple)

    def __del__(self):
        """
        Close up the DB connections and cursors

        :return:
        """
        # for each db name specified
        for db_name in self.db_names:
            # close the connection
            self.close_conn(db_name)

    def close_conn(self, db_name):
        """
        Closes a DB connection

        :param db_name:
        :return:
        """
        try:
            # if there is a connection, close it
            if self.dbs[db_name].conn is not None:
                # get the item out of the tuple
                conn = self.dbs[db_name].conn

                # close it
                conn.close()
        except Exception:
            self.logger.error('Error detected closing the %s DB connection.', db_name)

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

    def get_db_connection(self, db_info: namedtuple) -> bool:
        """
        Gets a connection to the DB. performs a check to continue trying until
        a connection is made.

        :return:
        """
        # init the connection status indicator
        good_conn: bool = False

        # until forever
        while not good_conn:
            try:
                # check the DB connection
                good_conn = self.check_db_connection(db_info)

                # try to get a connection if the check failed
                if not good_conn:
                    # try to connect to the DB
                    conn = psycopg2.connect(db_info.conn_str)

                    # set the autocommit on the connection
                    conn.autocommit = self.auto_commit

                    # create a new db info tuple
                    verified_tuple: namedtuple = self.db_info_tpl(db_info.name, db_info.conn_str, conn)

                    # check the new DB connection
                    good_conn = self.check_db_connection(verified_tuple)

                    # is the connection ok now?
                    if not good_conn:
                        self.logger.warning('DB Connection not established (auto commit %s) to %s.', self.auto_commit, db_info.name)
                    else:
                        self.logger.debug('DB Connection established (auto commit %s) to %s.', self.auto_commit, db_info.name)

                        # add the verified connection to the dict
                        self.dbs.update({db_info.name: verified_tuple})

                        # no need to continue
                        break

            except Exception:
                self.logger.error('Error getting connection %s.', db_info.name)
                good_conn = False

            # are we still looking for a connection
            if good_conn is False:
                self.logger.error('DB Connection failed to %s. Retrying...', db_info.name)
                time.sleep(5)

        # return pass/fail flag
        return good_conn

    def check_db_connection(self, db_info: namedtuple) -> bool:
        """
        Checks to see if there is a good connection to the DB.

        :param db_info:
        :return: boolean
        """
        # init the return value
        ret_val = None

        # init the cursor storage
        cursor = None

        try:
            # is there an existing connection
            if not db_info.conn:
                self.logger.warning('Existing DB connection not found for %s', db_info.name)

                # force getting a new connection
                ret_val = False
            else:
                # get the cursor
                cursor = db_info.conn.cursor()

                # get the DB version
                cursor.execute("SELECT version()")

                # get the value
                db_version = cursor.fetchone()

                # set the success (or not) flag
                ret_val = bool(db_version)

        except psycopg2.DatabaseError:
            self.logger.debug('Error database error checking DB connection.')

            # connection failed
            ret_val = False

        except psycopg2.InterfaceError:
            self.logger.debug('Error database interface error checking DB connection.')

            # connection failed
            ret_val = False

        except Exception:
            self.logger.debug('General DB connection issue. Probably connection time out.')

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
        success = self.get_db_connection(db_info)

        # did we get a connection
        if success:
            # init the cursor
            cursor = None

            try:
                # make sure the latest db_info is used
                db_info = self.dbs[db_name]

                # get a cursor
                cursor = db_info.conn.cursor()

                # execute the sql
                cursor.execute(sql_stmt)

                # get the returned value
                ret_val = cursor.fetchone()

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
            finally:
                # in there is a cursor, close it
                if cursor is not None:
                    # close it
                    cursor.close()

        else:
            # set the error code
            ret_val = -1

        # return to the caller
        return ret_val

    def commit(self, db_name: str):
        """
        issues a transaction commit

        :param db_name:
        :return:
        """
        # if this connection is set to not auto commit
        if not self.dbs[db_name].conn.autocommit:
            # issue the commit
            self.dbs[db_name].conn.commit()
