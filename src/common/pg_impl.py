# SPDX-FileCopyrightText: 2022 Renaissance Computing Institute. All rights reserved.
# SPDX-FileCopyrightText: 2023 Renaissance Computing Institute. All rights reserved.
# SPDX-FileCopyrightText: 2024 Renaissance Computing Institute. All rights reserved.
#
# SPDX-License-Identifier: GPL-3.0-or-later
# SPDX-License-Identifier: LicenseRef-RENCI
# SPDX-License-Identifier: MIT

"""
    Class for database functionalities

    Author: Phil Owen, RENCI.org
"""
from datetime import datetime, timedelta
from enum import Enum, EnumType
import json
import dateutil.parser
import pytz
import pandas as pd
import numpy as np
import requests
import html

from bs4 import BeautifulSoup
from urllib.parse import urlparse

from src.common.pg_utils_multi import PGUtilsMultiConnect
from src.common.logger import LoggingUtil


class PGImplementation(PGUtilsMultiConnect):
    """
        Class that contains DB calls for the ui data component.

        Note this class inherited from the PGUtilsMultiConnect class
        which has all the connection and cursor handling.
    """

    def __init__(self, db_names: tuple, _logger=None, _auto_commit=True):
        # if this has a reference to a logger passed in use it
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

    def get_wms_xml_data(self, wms_xml_url: str):
        """
        Gets/parses a WMS "get capabilities" data from the URL passes and puts it into the DB

        :return:
        """
        ret_val: int = 0

        # declare a prams object
        params: dict = {"format": "image/png", "transparent": True, "srs": "EPSG:3857", "legendURL": ""}

        # get the XML data
        response = requests.get(wms_xml_url)

        # if it went ok and there is data to parse
        if response.status_code == 200 and len(response.content) > 0:
            # load the XML data
            data = BeautifulSoup(response.text, "xml")

            # get all the supported layer types
            crs = data.find_all('CRS')

            # if the UI supports the layer type
            if len([x.text for x in crs if x.text == 'EPSG:3857']) > 0:
                # parse/extract the capabilities
                source: str = [x.text for x in data.find_all('Title')][0]

                # if no source data found
                if len(source) == 0:
                    # use the FQDN
                    source: str = urlparse(wms_xml_url).netloc

                # find the legend URL
                url: str = data.find('OnlineResource').get('xlink:href')

                # get all the layers
                layers = data.find_all('Layer')

                for layer in layers:
                    if layer.get('queryable') is not None and int(layer.get('queryable')) >= 0:
                        # get the title of the layer
                        name: str = layer.Title.text

                        # get the layer name
                        layer_name = layer.Name.text

                        # get the legend URL
                        self.get_legend_url(layer, params)

                        # insert the data into the DB
                        sql = (f"SELECT public.insert_external_layers(_name:='{name}', _source:='{source}', _url:='{url}', _layer:='{layer_name}', "
                               f"_params:='{json.dumps(params)}')")

                        # insert the layer details
                        ret_val = self.exec_sql('apsviz', sql)

                    # check for an insertion error
                    # if ret_val == -1:
                    #     break
            else:
                ret_val = -3
        else:
            # return a failure code
            ret_val = -2

        # return to the caller
        return ret_val

    @staticmethod
    def get_legend_url(layer, params):
        """
        Gets the legend URL

        :param layer:
        :param params:
        :return:
        """
        # empty this to prep for a fill of good data
        params['legendURL'] = ""

        # loop through the styles to find the legend
        for style in layer.find_all('Style'):
            # loop through the style names
            for style_name in style.find_all('Name'):
                # if this is the raster legend
                if style_name.text == 'raster/default' or len(style.find_all('Name')) == 1:
                    # if there is a legend URL in the data
                    if style.LegendURL is not None:
                        # get the URL
                        legend_url = style.LegendURL.OnlineResource.get('xlink:href')

                        # if a legend URL was found
                        if legend_url is not None:
                            # save it
                            params['legendURL'] = html.unescape(legend_url)

                            # no need to continue
                            break

    def get_map_workbench_data(self, **kwargs):
        """
        Gets the catalog workbench data

        :param kwargs:
        :return:
        """
        # init the return
        ret_val: dict = {}

        # if there was a run id specified in the request, we are returning a workbench for only that run
        if 'run_id' in kwargs and kwargs['run_id'] != 'null':
            # get the catalog members for the run using the id
            sql = f"SELECT public.get_catalog_workbench(_run_id:='{kwargs['run_id']}')"

            # get the layer list
            ret_val = self.exec_sql('apsviz', sql)
        # else go through the logic of determining the proper workbench
        else:
            # init the run id
            run_id: str = ''

            # create the SQL to get the latest runs for the workbench lookup
            sql: str = f"SELECT public.get_latest_runs(_insertion_date:={kwargs['insertion_date']}, _met_class:={kwargs['met_class']}, " \
                       f"_physical_location:={kwargs['physical_location']}, _ensemble_name:={kwargs['ensemble_name']}, " \
                       f"_instance_name:={kwargs['instance_name']}, _project_code:={kwargs['project_code']})"

            # get the max age
            max_age: int = int(kwargs['max_age'])

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
                # for each of the entries returned
                for item in ret_val:
                    # is there a tropical run and is it too old to display?
                    if item['met_class'] == 'tropical':
                        # get the number of days from the last tropical run to now
                        insertion_date = dateutil.parser.parse(item['insertion_date'])

                        # get the age (in days) of when this run occurred
                        date_diff = pytz.utc.localize(datetime.now()) - insertion_date

                        # is this young enough?
                        if date_diff.days < max_age:
                            # save the run id
                            run_id = f"{item['instance_id']}-{item['uid']}%"

                            # no need to continue
                            break
                    elif item['met_class'] == 'synoptic':
                        # save the run id
                        run_id = f"{item['instance_id']}-{item['uid']}%"

                # did we get a run id
                if run_id:
                    # get the catalog members for the run using the id
                    sql = f"SELECT public.get_catalog_workbench(_run_id:='{run_id}')"

                    # get the layer list
                    ret_val = self.exec_sql('apsviz', sql)
                else:
                    ret_val = {'Warning': 'No data found using the filter criteria selected.'}

        # return the data
        return ret_val

    def get_map_catalog_data(self, **kwargs):
        """
        gets the catalog data for the map UI

        :param **kwargs
        :return:
        """
        # init the return
        ret_val: dict = {}

        # get the new workbench data
        workbench_data: dict = self.get_workbench_data(**kwargs)

        # init the workbench SQL statement storage
        wb_sql: str = ""

        # should we continue?
        if not ('Error' in workbench_data or 'Warning' in workbench_data):
            # if the run id was specified, use it. this also disables using the new workbench code
            if 'run_id' in kwargs and kwargs['run_id'] != 'null':
                wb_sql = f", _run_id:='{kwargs['run_id']}'"
            # if there was workbench data, use it in the data query
            elif len(workbench_data) > 0:
                wb_sql: str = f",_run_id:='{'-'.join(workbench_data['workbench'][0].split('-')[:-1])}%'"

            # get the correct sp name
            if kwargs['use_v3_sp']:
                sp_name: str = 'public.get_terria_data_json_v3'
            else:
                sp_name: str = 'public.get_terria_data_json'

            # create the SQL
            sql: str = f"SELECT {sp_name}(_grid_type:={kwargs['grid_type']}, _event_type:={kwargs['event_type']}, " \
                       f"_instance_name:={kwargs['instance_name']}, _run_date:={kwargs['run_date']}, _end_date:={kwargs['end_date']}, " \
                       f"_limit:={kwargs['limit']}, _met_class:={kwargs['met_class']}, " \
                       f"_storm_name:={kwargs['storm_name']}, _cycle:={kwargs['cycle']}, _advisory_number:={kwargs['advisory_number']}, " \
                       f"_project_code:={kwargs['project_code']}, _product_type:={kwargs['product_type']}{wb_sql})"

            # get the layer list
            ret_val = self.exec_sql('apsviz', sql)

            # check the return
            if ret_val == -1:
                ret_val = {'Error': 'Database error getting catalog data.'}
            # check the return, no data gets a 404 return
            elif len(ret_val) == 0 or ret_val is None:
                # set a warning message
                ret_val = {'Warning': 'No data found using the filter criteria selected.'}
            else:
                # get the pull-down data using the above filtering mechanisms
                pulldown_data: dict = self.get_pull_down_data(**kwargs)

                # check the return, no data gets a 404 return
                if pulldown_data == -1 or pulldown_data is None:
                    # set a warning message
                    ret_val = {'Warning': 'No pulldown data found using the filter criteria selected.'}
                else:
                    # merge the pulldown data to the catalog list
                    ret_val.update({'pulldown_data': pulldown_data})

                    # if there is new workbench data, add it in now
                    if len(workbench_data) > 0:
                        # merge the workbench data to the catalog list
                        ret_val.update({'workbench': workbench_data['workbench']})

        # return the data
        return ret_val

    def get_workbench_data(self, **kwargs):
        """
        gets the workbench data from the DB using the filter params specified.

        :param kwargs:
        :return:
        """
        # init the return
        ret_val: dict = {}

        # are we using the new catalog workbench retrieval?
        if kwargs['use_new_wb']:
            # create the param list
            params: list = ['insertion_date', 'met_class', 'physical_location', 'instance_name', 'ensemble_name', 'project_code']

            # loop through the params for the SP
            for param in params:
                # if the param is already in the kwargs use it, otherwise null it out
                if param not in kwargs:
                    # add this parm to the list
                    kwargs.update({param: 'null'})

            # add in the max age int
            kwargs.update({'max_age': 1})

            # try to make the call for records
            ret_val = self.get_map_workbench_data(**kwargs)

            # check the return
            if ret_val == -1:
                ret_val = {'Error': 'Database error getting catalog workbench data.'}
            # check the return, no data gets a 404 return
            elif len(ret_val) == 0 or ret_val is None:
                # set a warning message
                ret_val = {'Warning': 'No workbench data found using the filter criteria selected.'}

        # return the data to the caller
        return ret_val

    def get_external_layers(self) -> dict:
        """
        gets the pulldown data given the list of filtering mechanisms passed.

        :return:
        """
        # init the return value
        ret_val: dict = {}

        # get the pull-down data
        sql = "SELECT * FROM public.get_external_layers_json();"

        # get the pulldown data
        ret_val = self.exec_sql('apsviz', sql)

        # make sure this is not an array if only one meteorological class is returned
        if ret_val == -1:
            ret_val = {}

        # return the full dataset to the caller
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

        # create the SQL
        sql: str = f"SELECT public.get_catalog_member_records(_run_id := {kwargs['run_id']}, _project_code := {kwargs['project_code']}, " \
                   f"_filter_event_type := {kwargs['filter_event_type']}, _limit := {kwargs['limit']});"

        # get the layer list
        ret_val = self.exec_sql('apsviz', sql)

        # return the data
        return ret_val

    def get_station_tidal_level_offset(self, station_id: int, instance_name: str) -> float:
        """
        gets the tidal level offset for a station

        :param station_id:
        :param instance_name:
        :return:
        """
        # init the return value
        ret_val: float = 0.0

        # create the SQL
        sql: str = f"SELECT * FROM public.get_station_tidal_level_offset(_station_id := {station_id}, _instance_name := '{instance_name}');"

        # get the layer list
        ret_val = self.exec_sql('apsviz', sql)

        # if the call was unsuccessful
        if ret_val == -1:
            ret_val = 0.0

        # return to the caller
        return ret_val

    def add_tidal_datum_level_offset(self, station_id: int, instance_name: str, station_df: pd.DataFrame):
        """
        get the tidal datum level offset value for a station and add it to the appropriate data columns

        :param station_id:
        :param instance_name:
        :param station_df:
        :return:
        """
        # get the tidal level offsets at the location
        tidal_offset: float = self.get_station_tidal_level_offset(station_id, instance_name)

        # if there was a tidal level offset found for that point
        if tidal_offset != 0:
            # for each target column to get the treatment
            for col in ['Observations', 'NOAA Tidal Predictions']:
                # if the col exists
                if col in station_df.columns:
                    # add in the level offset
                    station_df[col] = station_df[col] + tidal_offset

    def get_station_data(self, **kwargs) -> str:
        """
        gets the station data.

        :param kwargs:
        :return:
        """
        # calculate max_forecast_endtime from time_mark
        max_forecast_endtime = (datetime.fromisoformat(kwargs['time_mark']) + timedelta(14)).isoformat()

        # get forecast data
        forecast_data = self.get_forecast_station_data(kwargs['station_name'], kwargs['time_mark'], max_forecast_endtime, kwargs['data_source'],
                                                       kwargs['instance_name'])

        # derive start date from the time mark
        start_date = (datetime.fromisoformat(kwargs['time_mark']) - timedelta(4)).isoformat()

        # check for an error
        if forecast_data.empty:
            # If no forecast data add 120 hours (5 days) to end_date for the tidal predictions data
            end_date = (datetime.fromisoformat(kwargs['time_mark']) + timedelta(5)).isoformat()
        else:
            # get end_date from last datetime in forecast data
            end_date = forecast_data['time_stamp'].iloc[-1]

        # get nowcast data_source from forecast data_source
        # check if data_source is tropical
        if kwargs['forcing_metclass'] == 'tropical':
            # if tropical split data source and replace second value (OFCL) with NOWCAST
            source_parts = kwargs['data_source'].split('_')
            source_parts[1] = 'NOWCAST'
            nowcast_source = "_".join(source_parts)
        else:
            # if synoptic split data source and replace fist value (GFSFORECAST) with NOWCAST
            nowcast_source = 'NOWCAST_' + "_".join(kwargs['data_source'].split('_')[1:])

        # get obs data
        obs_data = self.get_obs_station_data(kwargs['station_name'], start_date, end_date)

        # drop empty columns
        empty_cols = [col for col in obs_data.columns if obs_data[col].isnull().all()]
        obs_data.drop(empty_cols, axis=1, inplace=True)

        # get nowcast data
        nowcast_data = self.get_nowcast_station_data(kwargs['station_name'], start_date, end_date, nowcast_source, kwargs['instance_name'])

        # If nowcast data exists, merge it with obs data
        if not nowcast_data.empty:
            if not obs_data.empty:
                # Check if for type of observations name and make appropriate changes
                observation_name = self.get_obs_data_name(obs_data)

                # Merge nowcast data with Obs data
                obs_data = obs_data.merge(nowcast_data, on='time_stamp', how='outer')
            else:
                # use nowcast data as obs data in cases where there is no obs data
                obs_data = nowcast_data
                observation_name = None
        else:
            if not obs_data.empty:
                # Check if for type of observations name and make appropriate changes
                observation_name = self.get_obs_data_name(obs_data)
            else:
                # if obs_data empty observation_name = None
                observation_name = None

        # replace any None values with np.nan, in both DataFrames
        forecast_data.fillna(value=np.nan)
        obs_data.fillna(value=np.nan)

        # replace any -99999 values with np.nan, in both DataFrames
        f_cols = forecast_data.columns.tolist()
        forecast_data[f_cols] = forecast_data[f_cols].replace([-99999], np.nan)
        o_cols = obs_data.columns.tolist()
        obs_data[o_cols] = obs_data[o_cols].replace([-99999], np.nan)

        # convert all values after the time mark to nan, in obs data, except in the time_stamp and tidal_predictions columns
        for col in obs_data.columns:
            if col not in ('time_stamp', 'tidal_predictions', 'tidal_gauge_water_level'):
                timemark = " ".join(kwargs['time_mark'].split('T'))
                obs_data.loc[obs_data.time_stamp >= timemark, col] = np.nan
            else:
                continue

        # check for an error
        if not forecast_data.empty:
            # merge the obs DataFrame with the forecast Dataframe
            station_df = obs_data.merge(forecast_data, on='time_stamp', how='outer')

            # get the forecast column name, and rename it
            forecast_column_name = "".join(kwargs['data_source'].split('.')).lower()
            station_df.rename(columns={forecast_column_name: 'APS Forecast'}, inplace=True)
        else:
            station_df = obs_data

        # get the nowcast column name
        nowcast_column_name = "".join(nowcast_source.split('.')).lower()

        # check if nowcast column exists
        if nowcast_column_name in station_df.columns:
            if observation_name:
                # get difference between observation and nowcast columns
                station_df['Difference (APS-OBS)'] = station_df[nowcast_column_name] - station_df[observation_name]

                # rename the columns
                station_df.rename(columns={'time_stamp': 'time', nowcast_column_name: 'APS Nowcast', observation_name: 'Observations',
                                           'tidal_predictions': 'NOAA Tidal Predictions'}, inplace=True)
            else:
                # rename the columns
                station_df.rename(columns={'time_stamp': 'time', nowcast_column_name: 'APS Nowcast', 'tidal_predictions': 'NOAA Tidal Predictions'},
                                  inplace=True)
        else:
            # else check if observation_name has a value and rename the columns
            if observation_name:
                station_df.rename(columns={'time_stamp': 'time', observation_name: 'Observations', 'tidal_predictions': 'NOAA Tidal Predictions'},
                                  inplace=True)
            else:
                # else if observation name is None rename tidal_predictions
                station_df.rename(columns={'time_stamp': 'time', 'tidal_predictions': 'NOAA Tidal Predictions'}, inplace=True)

        # add the station level offsets (if they exist)
        self.add_tidal_datum_level_offset(kwargs['station_name'], kwargs['instance_name'], station_df)

        # return the data to the caller
        return station_df.to_csv(index=False)

    def get_forecast_station_data(self, station_name, time_mark, max_forecast_endtime, data_source, instance_name) -> pd.DataFrame:
        """
        Gets the forcast station data

        :param station_name:
        :param time_mark:
        :param max_forecast_endtime:
        :param data_source:
        :param instance_name:
        :return:
        """
        # init the return value:
        ret_val: pd.DataFrame = pd.DataFrame()

        # Run the query
        sql = f"SELECT * FROM get_forecast_timeseries_station_data(_station_name := '{station_name}', _timemark := '{time_mark}', " \
              f"_max_forecast_endtime := '{max_forecast_endtime}', _data_source := '{data_source}',  _source_instance := '{instance_name}')"

        # get the info
        station_data = self.exec_sql('apsviz_gauges', sql)

        # was it successful?
        if station_data != -1:
            # convert query output to Pandas dataframe
            ret_val = pd.DataFrame.from_dict(station_data, orient='columns')
        else:
            ret_val = pd.DataFrame(None)

        # Return Pandas dataframe
        return ret_val

    def get_nowcast_station_data(self, station_name, start_date, end_date, data_source, instance_name) -> pd.DataFrame:
        """
        Gets the forcast station data

        :param station_name:
        :param start_date:
        :param end_date:
        :param data_source:
        :param instance_name:
        :return:
        """
        # init the return value:
        ret_val: pd.DataFrame = pd.DataFrame()

        # Run the query
        sql = f"SELECT * FROM get_nowcast_timeseries_station_data(_station_name := '{station_name}', _start_date := '{start_date}', " \
              f"_end_date := '{end_date}', _data_source := '{data_source}',  _source_instance := '{instance_name}')"

        # get the info
        station_data = self.exec_sql('apsviz_gauges', sql)

        # was it successful?
        if station_data != -1:
            # convert query output to Pandas dataframe
            ret_val = pd.DataFrame.from_dict(station_data, orient='columns')
        else:
            ret_val = pd.DataFrame(None)

        # Return Pandas dataframe
        return ret_val

    @staticmethod
    def get_obs_data_name(obs_data):
        """
        Gets the observed name.

        :param obs_data:
        :return: observation_name
        """
        # check if there is a wave height ocean buoy in the data
        if 'ocean_buoy_wave_height' in obs_data.columns:
            # save the observation name
            observation_name = [s for s in obs_data.columns.values if 'wave_height' in s][0]
        else:
            # save the observation name
            observation_name = [s for s in obs_data.columns.values if 'water_level' in s]

            # if there was no observation name found
            if len(observation_name) == 0:
                # empty the name
                observation_name = None
            else:
                # save the name
                observation_name = observation_name[0]

        # return the name to the caller
        return observation_name

    def get_obs_station_data(self, station_name, start_date, end_date) -> pd.DataFrame:
        """
        Gets the observed station data.

        :param station_name:
        :param start_date:
        :param end_date:
        :return:
        """
        # init the return value:
        ret_val: pd.DataFrame = pd.DataFrame()

        # build the query
        sql = f"SELECT * FROM get_obs_timeseries_station_data(_station_name := '{station_name}', _start_date := '{start_date}', " \
              f"_end_date := '{end_date}')"

        # get the info
        station_data = self.exec_sql('apsviz_gauges', sql)

        # was it successful?
        if station_data != -1:
            # convert query output to Pandas dataframe
            ret_val = pd.DataFrame.from_dict(station_data, orient='columns')

        # Return Pandas dataframe
        return ret_val

    def get_instance_names(self, name: str, project_code: str = None) -> EnumType:
        """
        Gets an Enum list of instance names for the UI pulldown

        :param name:
        :param project_code:
        :return:
        """
        # init the return value:
        ret_val = None

        # prep the param for the SP
        if project_code is None:
            project_code = 'null'
        else:
            project_code = f"'{project_code}'"

        # build the query
        sql = f"SELECT * FROM get_instance_names(_project_code := {project_code});"

        # get the info
        enum_data = self.exec_sql('apsviz', sql)

        # was it successful?
        if enum_data != -1:
            # convert query output to Pandas dataframe
            ret_val = Enum(name, enum_data)

        # Return Pandas dataframe
        return ret_val

    def verify_user(self, email: str) -> dict:
        """
        verifies the user has an account and the password is correct.

        if the verification is successful, return a JSON object with pass/fail and user account data

        :param email:
        :return:
        """
        # init the return value
        ret_val = None

        # prep the email param for the SP
        if email is None:
            email = 'null'
        else:
            email = f"'{email}'"

        # build the query. this will also return the user's profile
        sql = f"SELECT verify_user(_email := {email});"

        # get the info
        ret_val = self.exec_sql('apsviz', sql)

        # return the result of the inquiry
        return ret_val

    def add_user(self, **kwargs) -> dict:
        """
        Adds the user and profile and returns a pass/fail dict

        if the call is successful, returns a pass/fail dict

        :param:
        :return:
        """
        # init the return value:
        ret_val = None

        # build the query
        sql = (f"SELECT public.add_user(_email:={kwargs['email']}, _password_hash:={kwargs['password_hash']}, _role_id:={kwargs['role_id']}, "
               f"_details:={kwargs['details']}, _maxelestyle:={kwargs['maxele_style']}, _maxwvelstyle:={kwargs['maxwvel_style']}, "
               f"_swanstyle:={kwargs['swan_style']});")

        # get the info
        ret_val = self.exec_sql('apsviz', sql)

        # Return Pandas dataframe
        return ret_val

    def update_user(self, **kwargs) -> dict:
        """
        Updates the user profile and returns a pass/fail dict

        if the call is successful, returns a pass/fail dict

        :param :

        :return:
        """
        # init the return value:
        ret_val = None

        # create the SQL query
        sql = (f"SELECT public.update_user(_email:={kwargs['email']}, _password_hash:={kwargs['password_hash']}, _role_id:={kwargs['role_id']}, "
               f"_details:={kwargs['details']}, _maxelestyle:={kwargs['maxelestyle']}, _maxwvelstyle:={kwargs['maxwvelstyle']}, _swanstyle:={kwargs['swanstyle']});")

        # get the info
        ret_val = self.exec_sql('apsviz', sql)

        # Return Pandas dataframe
        return ret_val
