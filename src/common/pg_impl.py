# SPDX-FileCopyrightText: 2022 Renaissance Computing Institute. All rights reserved.
# SPDX-FileCopyrightText: 2023 Renaissance Computing Institute. All rights reserved.
#
# SPDX-License-Identifier: GPL-3.0-or-later
# SPDX-License-Identifier: LicenseRef-RENCI
# SPDX-License-Identifier: MIT

"""
    Class for database functionalities

    Author: Phil Owen, RENCI.org
"""
from src.common.pg_utils_multi import PGUtilsMultiConnect
from src.common.logger import LoggingUtil


class PGImplementation(PGUtilsMultiConnect):
    """
        Class that contains DB calls for the Archiver.

        Note this class inherits from the PGUtilsMultiConnect class
        which has all the connection and cursor handling.
    """

    def __init__(self, db_names: tuple, _logger=None, _auto_commit=True):
        # if a reference to a logger passed in use it
        if _logger is not None:
            # get a handle to a logger
            self.logger = _logger
        else:
            # get the log level and directory from the environment.
            log_level, log_path = LoggingUtil.prep_for_logging()

            # create a logger
            self.logger = LoggingUtil.init_logging("APSViz.UI-data.PGImplementation", level=log_level, line_format='medium', log_file_path=log_path)

        # init the base class
        PGUtilsMultiConnect.__init__(self, 'APSViz.Settings', db_names, _logger=self.logger, _auto_commit=_auto_commit)

    def __del__(self):
        """
        Calls super base class to clean up DB connections and cursors.

        :return:
        """
        # clean up connections and cursors
        PGUtilsMultiConnect.__del__(self)

    def get_terria_map_catalog_data(self, **kwargs):
        """
        gets the catalog data for the terria map UI

        :return:
        """
        # init the return
        catalog_list: dict = {}

        # create the sql
        sql: str = f"SELECT public.get_terria_data_json(_grid_type:={kwargs['grid_type']}, _event_type:={kwargs['event_type']}, " \
                   f"_instance_name:={kwargs['instance_name']}, _run_date:={kwargs['run_date']}, _end_date:={kwargs['end_date']}, " \
                   f"_limit:={kwargs['limit']}, _met_class:={kwargs['met_class']}, _storm_name:={kwargs['storm_name']}, " \
                   f"_cycle:={kwargs['cycle']}, _advisory_number:={kwargs['advisory_number']})"

        # get the layer list
        catalog_list = self.exec_sql('apsviz', sql)

        # get the pull-down data using the above filtering mechanisms
        pulldown_data: dict = self.get_pull_down_data(**kwargs)

        # merge the pulldown data to the catalog list
        catalog_list.update({'pulldown_data': pulldown_data})

        # return the data
        return catalog_list

    def get_pull_down_data(self, **kwargs) -> dict:
        """
        gets the pulldown data given the list of filtering mechanisms passed.

        :param kwargs:
        :return:
        """
        # init the return value
        pulldown_data: dict = {}

        # get the pull-down data
        sql = f"SELECT public.get_terria_pulldown_data(_grid_type:={kwargs['grid_type']}, _event_type:={kwargs['event_type']}, " \
              f"_instance_name:={kwargs['instance_name']}, _met_class:={kwargs['met_class']}, _storm_name:={kwargs['storm_name']}, " \
              f"_cycle:={kwargs['cycle']}, _advisory_number:={kwargs['advisory_number']});"

        # get the pulldown data
        pulldown_data = self.exec_sql('apsviz', sql)

        # return the full dataset to the caller
        return pulldown_data

    def get_obs_station_data(self, **kwargs):
        """
        gets the obs station data.

        :param kwargs:
        :return:
        """
        # init the return
        observations_list: dict = {}

        # create the sql
        sql: str = f"SELECT public.get_obs_station_data(_station_name := {kwargs['station_name']}, _start_date := {kwargs['start_date']}, " \
                   f"_end_date := {kwargs['end_date']});"

        # get the layer list
        observations_list = self.exec_sql('apsviz_gauges', sql)

        # return the data
        return observations_list