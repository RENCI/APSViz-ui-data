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
from datetime import datetime, timedelta
import dateutil.parser
import pytz
import pandas as pd
import numpy as np

from src.common.pg_utils_multi import PGUtilsMultiConnect
from src.common.logger import LoggingUtil


class PGImplementation(PGUtilsMultiConnect):
    """
        Class that contains DB calls for the ui data component.

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
        PGUtilsMultiConnect.__init__(self, 'APSViz.UI-data.PGImplementation', db_names, _logger=self.logger, _auto_commit=_auto_commit)

    def __del__(self):
        """
        Calls super base class to clean up DB connections and cursors.

        :return:
        """
        # clean up connections and cursors
        PGUtilsMultiConnect.__del__(self)

    def get_terria_map_workbench_data(self, **kwargs):
        """
        Gets the catalog workbench data

        :param kwargs:
        :return:
        """
        # init the return
        ret_val: dict = {}

        # create the sql to get the latest runs for the workbench lookup
        sql: str = f"SELECT public.get_latest_runs(_insertion_date:={kwargs['insertion_date']}, _met_class:={kwargs['met_class']}, " \
                   f"_physical_location:={kwargs['physical_location']}, _ensemble_name:={kwargs['ensemble_name']}, _project_code:=" \
                   f"{kwargs['project_code']})"

        # get the max age
        max_age: int = int(kwargs['max_age'])

        # get the layer list
        ret_val = self.exec_sql('asgs', sql)

        # check the return
        if ret_val == -1:
            ret_val = {'Error': 'Database error getting catalog workbench data.'}
        # check the return, no data gets a 404 return
        elif len(ret_val) == 0 or ret_val is None:
            # set a warning message
            ret_val = {'Warning': 'No data found using the filter criteria selected.'}
        else:
            # for each of the entries returned
            for item in ret_val:
                # interrogate the results we are looking to get this down to the latest relevant tropical run
                # or the latest synoptic run catalog member.

                # is there a tropical run and is it too old to display
                if item['met_class'] == 'tropical':
                    # get the number of days from the last tropical run to now
                    insertion_date = dateutil.parser.parse(item['insertion_date'])
                    date_diff = pytz.utc.localize(datetime.utcnow()) - insertion_date

                    # is this young enough
                    if date_diff.days < max_age:
                        # save the run id
                        run_id = f"{item['instance_id']}-{item['uid']}%"

                        # no need to continue
                        break
                elif item['met_class'] == 'synoptic':
                    # save the run id
                    run_id = f"{item['instance_id']}-{item['uid']}%"

            # get the catalog members for the run using the id
            sql = f"SELECT public.get_catalog_workbench(_run_id:='{run_id}')"

            # get the layer list
            ret_val = self.exec_sql('apsviz', sql)

        # return the data
        return ret_val

    def get_terria_map_catalog_data(self, **kwargs):
        """
        gets the catalog data for the terria map UI

        :param **kwargs
        :return:
        """
        # init the return
        ret_val: dict = {}

        # create the sql
        sql: str = f"SELECT public.get_terria_data_json(_grid_type:={kwargs['grid_type']}, _event_type:={kwargs['event_type']}, " \
                   f"_instance_name:={kwargs['instance_name']}, _run_date:={kwargs['run_date']}, _end_date:={kwargs['end_date']}, " \
                   f"_limit:={kwargs['limit']}, _met_class:={kwargs['met_class']}, _storm_name:={kwargs['storm_name']}, " \
                   f"_cycle:={kwargs['cycle']}, _advisory_number:={kwargs['advisory_number']}, _project_code:={kwargs['project_code']}, " \
                   f"_product_type:={kwargs['product_type']})"

        # get the layer list
        ret_val = self.exec_sql('apsviz', sql)

        # check the return
        if ret_val == -1:
            ret_val = {'Error': 'Database error getting catalog workbench data.'}
        # check the return, no data gets a 404 return
        elif len(ret_val) == 0 or ret_val is None:
            # set a warning message
            ret_val = {'Warning': 'No data found using the filter criteria selected.'}
        else:
            # get the pull-down data using the above filtering mechanisms
            pulldown_data: dict = self.get_pull_down_data(**kwargs)

            # check the return
            if pulldown_data == -1:
                ret_val = {'Error': 'Database error getting pulldown data.'}
            # check the return, no data gets a 404 return
            elif len(ret_val) == 0 or ret_val is None:
                # set a warning message
                ret_val = {'Warning': 'No pulldown data found using the filter criteria selected.'}
            else:
                # merge the pulldown data to the catalog list
                ret_val.update({'pulldown_data': pulldown_data})

                # are we using the new catalog workbench retrieval
                if kwargs['use_new_wb']:
                    # init the kwargs variable
                    #kwargs: dict = {}

                    # create the param list
                    params: list = ['insertion_date', 'met_class', 'physical_location', 'ensemble_name', 'project_code']

                    # loop through the params for the SP
                    for param in params:
                        # if the param is already in the kwargs use it, otherwise null it out
                        if param not in kwargs:
                            # add this parm to the list
                            kwargs.update({param: 'null'})

                    # add in the max age int
                    kwargs.update({'max_age': 2})

                    # try to make the call for records
                    new_workbench_data = self.get_terria_map_workbench_data(**kwargs)

                    # check the return
                    if new_workbench_data == -1:
                        ret_val = {'Error': 'Database error getting catalog workbench data.'}
                    # check the return, no data gets a 404 return
                    elif len(new_workbench_data) == 0 or new_workbench_data is None:
                        # set a warning message
                        ret_val = {'Warning': 'No data found using the filter criteria selected.'}
                    else:
                        # merge the workbench data to the catalog list
                        ret_val.update({'workbench': new_workbench_data['workbench']})

        # return the data
        return ret_val

    def get_pull_down_data(self, **kwargs) -> dict:
        """
        gets the pulldown data given the list of filtering mechanisms passed.

        :param kwargs:
        :return:
        """
        # init the return value
        ret_val: dict = {}

        # get the pull-down data
        sql = f"SELECT public.get_terria_pulldown_data(_grid_type:={kwargs['grid_type']}, _event_type:={kwargs['event_type']}, " \
              f"_instance_name:={kwargs['instance_name']}, _met_class:={kwargs['met_class']}, _storm_name:={kwargs['storm_name']}, " \
              f"_cycle:={kwargs['cycle']}, _advisory_number:={kwargs['advisory_number']}, " \
              f"_run_date:={kwargs['run_date']}, _end_date:={kwargs['end_date']}, " \
              f"_project_code:={kwargs['project_code']}, _product_type:={kwargs['product_type']});"

        # get the pulldown data
        ret_val = self.exec_sql('apsviz', sql)

        # make sure this is not an array if only one meteorological class is returned
        if ret_val != -1 and len(ret_val) == 1:
            ret_val = ret_val[0]

        # return the full dataset to the caller
        return ret_val

    def get_catalog_member_records(self, **kwargs) -> dict:
        """
        gets the apsviz catalog member record for the run id and project code passed. the SP default
        record count returned can be overridden.

        :param **kwargs
        :return:
        """
        # init the return
        ret_val: dict = {}

        # create the sql
        sql: str = f"SELECT public.get_catalog_member_records(_run_id := {kwargs['run_id']}, _project_code := {kwargs['project_code']}, " \
                   f"_filter_event_type := {kwargs['filter_event_type']}, _limit := {kwargs['limit']});"

        # get the layer list
        ret_val = self.exec_sql('apsviz', sql)

        # return the data
        return ret_val

    def get_station_data(self, **kwargs) -> str:
        """
        gets the station data.

        :param kwargs:
        :return:
        """
        # get forecast data
        forecast_data = self.get_forecast_station_data(kwargs['station_name'], kwargs['time_mark'], kwargs['data_source'])

        # check for an error
        if forecast_data.empty:
            return ''

        # derive start date from the time mark
        start_date = (datetime.fromisoformat(kwargs['time_mark']) - timedelta(4)).isoformat()

        # get end_date from last datetime in forecast data
        end_date = forecast_data['time_stamp'].iloc[-1]

        # get nowcast data_source from forecast data_source
        nowcast_source = 'NOWCAST_' + "_".join(kwargs['data_source'].split('_')[1:])

        # get obs and nowcast data
        obs_data = self.get_obs_station_data(kwargs['station_name'], start_date, end_date, nowcast_source)

        # drop empty columns
        empty_cols = [col for col in obs_data.columns if obs_data[col].isnull().all()]
        obs_data.drop(empty_cols, axis=1, inplace=True)

        # replace any None values with np.nan, in both DataFrames
        forecast_data.fillna(value=np.nan)
        obs_data.fillna(value=np.nan)

        # convert all values after the time mark to nan, in obs data, except in the time_stamp and tidal_predictions columns
        for col in obs_data.columns:
            if col not in ('time_stamp', 'tidal_predictions'):
                obs_data.loc[obs_data.time_stamp >= kwargs['time_mark'], col] = np.nan
            else:
                continue

        # merge the obs DataFrame with the forecast Dataframe
        station_df = obs_data.merge(forecast_data, on='time_stamp', how='outer')

        # get the forecast and nowcast column names
        forecast_column_name = "".join(kwargs['data_source'].split('.')).lower()
        nowcast_column_name = "".join(nowcast_source.split('.')).lower()

        # rename the columns
        station_df.rename(columns={forecast_column_name: 'forecast_water_level', nowcast_column_name: 'nowcast_water_level'}, inplace=True)

        # return the data to the caller
        return station_df.to_csv(index=False)

    def get_forecast_station_data(self, station_name, time_mark, data_source) -> pd.DataFrame:
        """
        Gets the forcast station data

        :param station_name:
        :param time_mark:
        :param data_source:
        :return:
        """
        # init the return value:
        ret_val: pd.DataFrame = pd.DataFrame()

        # Run query
        sql = f"SELECT * FROM get_forecast_timeseries_station_data(_station_name := '{station_name}', _timemark := '{time_mark}', " \
              f"_data_source := '{data_source}')"

        # get the info
        station_data = self.exec_sql('apsviz_gauges', sql)

        # was it successful
        if station_data != -1:
            # convert query output to Pandas dataframe
            ret_val = pd.DataFrame.from_dict(station_data, orient='columns')
        else:
            ret_val = pd.DataFrame(None)

        # Return Pandas dataframe
        return ret_val

    def get_obs_station_data(self, station_name, start_date, end_date, nowcast_source) -> pd.DataFrame:
        """
        Gets the observed station data.

        :param station_name:
        :param start_date:
        :param end_date:
        :param nowcast_source:
        :return:
        """
        # init the return value:
        ret_val: pd.DataFrame = pd.DataFrame()

        # build the query
        sql = f"SELECT * FROM get_obs_timeseries_station_data(_station_name := '{station_name}', _start_date := '{start_date}', _end_date := " \
              f"'{end_date}', _nowcast_source := '{nowcast_source}')"

        # get the info
        station_data = self.exec_sql('apsviz_gauges', sql)

        # was it successful
        if station_data != -1:
            # convert query output to Pandas dataframe
            ret_val = pd.DataFrame.from_dict(station_data, orient='columns')

        # Return Pandas dataframe
        return ret_val
