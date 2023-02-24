# SPDX-FileCopyrightText: 2022 Renaissance Computing Institute. All rights reserved.
# SPDX-FileCopyrightText: 2023 Renaissance Computing Institute. All rights reserved.
#
# SPDX-License-Identifier: GPL-3.0-or-later
# SPDX-License-Identifier: LicenseRef-RENCI
# SPDX-License-Identifier: MIT

"""
    Logger methods for all components
"""

import os
import logging
from logging.handlers import RotatingFileHandler


class LoggingUtil:
    """
    creates and configures a logger
    """

    @staticmethod
    def get_log_path() -> str:
        """
        gets the log path

        :return:
        """
        # return the log path
        return os.getenv('LOG_PATH', os.path.dirname(__file__))

    @staticmethod
    def init_logging(name, level=None, line_format='short', log_file_path=None):
        """
            Logging utility controlling format and setting initial logging level
        """
        # get a new logger
        logger = logging.getLogger(__name__)

        # is this the root
        if not logger.parent.name == 'root':
            return logger

        # get the log level and directory from the environment
        if level is None:
            level: int = int(os.getenv('LOG_LEVEL', str(logging.INFO)))

        if log_file_path is None:
            log_file_path: str = os.getenv('LOG_PATH', os.path.dirname(__file__))

        # create the dir if it does not exist
        if not os.path.exists(log_file_path):
            os.mkdir(log_file_path)

        # define the various output formats
        format_type = {"minimum": '%(message)s', "short": '%(funcName)s(): %(message)s', "medium": '%(asctime)-15s - %(funcName)s(): %(message)s',
                       "long": '%(asctime)-15s  - %(filename)s %(funcName)s() %(levelname)s: %(message)s'}[line_format]

        # create a stream handler (default to console)
        stream_handler = logging.StreamHandler()

        # create a formatter
        formatter = logging.Formatter(format_type)

        # set the formatter on the console stream
        stream_handler.setFormatter(formatter)

        # get the name of this logger
        logger = logging.getLogger(name)

        # set the logging level
        logger.setLevel(level)

        # if there was a file path passed in use it
        if log_file_path is not None:
            # create a rotating file handler, 100mb max per file with a max number of 10 files
            file_handler = RotatingFileHandler(filename=os.path.join(log_file_path, name + '.log'), maxBytes=1000000, backupCount=10)

            # set the formatter
            file_handler.setFormatter(formatter)

            # set the log level
            file_handler.setLevel(level)

            # add the handler to the logger
            logger.addHandler(file_handler)

        # add the console handler to the logger
        logger.addHandler(stream_handler)

        # return to the caller
        return logger
