# SPDX-FileCopyrightText: 2022 Renaissance Computing Institute. All rights reserved.
# SPDX-FileCopyrightText: 2023 Renaissance Computing Institute. All rights reserved.
# SPDX-FileCopyrightText: 2024 Renaissance Computing Institute. All rights reserved.
#
# SPDX-License-Identifier: GPL-3.0-or-later
# SPDX-License-Identifier: LicenseRef-RENCI
# SPDX-License-Identifier: MIT

"""
    APSVIZ UI Data server.
"""

import json
import os
import uuid
import csv

from typing import Union

from fastapi import FastAPI, Query, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, FileResponse, PlainTextResponse
from starlette.background import BackgroundTask

from src.common.logger import LoggingUtil
from src.common.pg_impl import PGImplementation
from src.common.security import Security
from src.common.bearer import JWTBearer
from src.common.utils import GenUtils, BrandName
from src.common.geopoints import GeoPoint

# set the app version
app_version = os.getenv('APP_VERSION', 'Version number not set')

# declare the FastAPI details
APP = FastAPI(title='APSVIZ UI Data', version=app_version)

# get the log level and directory from the environment.
log_level, log_path = LoggingUtil.prep_for_logging()

# create a logger
logger = LoggingUtil.init_logging("APSVIZ.UI-data.UI", level=log_level, line_format='medium', log_file_path=log_path)

# declare app access details
APP.add_middleware(CORSMiddleware, allow_origins=['*'], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

# declare the database to use
db_name: tuple = ('apsviz', 'apsviz_gauges')

# create a DB connection object
db_info: PGImplementation = PGImplementation(db_name, _logger=logger)

# create a Security object
security = Security()


@APP.get('/get_ui_data', status_code=200, response_model=None)
async def get_ui_data(grid_type: Union[str, None] = Query(default=None), event_type: Union[str, None] = Query(default=None),
                                      instance_name: Union[str, None] = Query(default=None), met_class: Union[str, None] = Query(default=None),
                                      storm_name: Union[str, None] = Query(default=None), cycle: Union[str, None] = Query(default=None),
                                      advisory_number: Union[str, None] = Query(default=None), run_date: Union[str, None] = Query(default=None),
                                      end_date: Union[str, None] = Query(default=None), project_code: Union[str, None] = Query(default=None),
                                      ensemble_name: Union[str, None] = Query(default=None), product_type: Union[str, None] = Query(default=None),
                                      limit: Union[int, None] = Query(default=7), use_new_wb: Union[bool, None] = Query(default=False),
                                      use_v3_sp: Union[bool, None] = Query(default=False)) -> json:
    """
    Gets the json formatted map UI catalog data.
    <br/>Note: Leave filtering params empty if not desired.
    <br/>&nbsp;&nbsp;&nbsp;grid_type: Filter by the name of the ECFLOW grid
    <br/>&nbsp;&nbsp;&nbsp;event_type: Filter by the event type
    <br/>&nbsp;&nbsp;&nbsp;instance_name: Filter by the name of the ECFLOW instance
    <br/>&nbsp;&nbsp;&nbsp;met_class: Filter by the meteorological class
    <br/>&nbsp;&nbsp;&nbsp;storm_name: Filter by the storm name
    <br/>&nbsp;&nbsp;&nbsp;cycle: Filter by the cycle
    <br/>&nbsp;&nbsp;&nbsp;advisory_number: Filter by the advisory number
    <br/>&nbsp;&nbsp;&nbsp;run_date: Filter by the run date in the form of yyyy-mm-dd
    <br/>&nbsp;&nbsp;&nbsp;end_date: Filter by the data between the run date and end date
    <br/>&nbsp;&nbsp;&nbsp;project_code: Filter by the project code
    <br/>&nbsp;&nbsp;&nbsp;ensemble_name: The name of the run ensemble
    <br/>&nbsp;&nbsp;&nbsp;product_type: Filter by the product type
    <br/>&nbsp;&nbsp;&nbsp;limit: Limit the number of catalog records returned in days (default is 7)
    <br/>&nbsp;&nbsp;&nbsp;use_new_wb: Use the new catalog workbench code
    <br/>&nbsp;&nbsp;&nbsp;use_v3_sp: Use the new v3 data stored procedure
    """
    # pylint: disable=unused-argument
    # pylint: disable=too-many-arguments
    # pylint: disable=too-many-locals

    # init the returned data and html status code
    ret_val: dict = {}
    status_code: int = 200

    try:
        # init the kwargs variable
        kwargs: dict = {}

        # create the param list
        params: list = ['grid_type', 'event_type', 'instance_name', 'met_class', 'storm_name', 'cycle', 'advisory_number', 'run_date', 'end_date',
                        'project_code', 'product_type', 'limit', 'ensemble_name']

        # loop through the SP params passed in
        for param in params:
            # add this parm to the list
            kwargs.update({param: 'null' if not locals()[param] else f"'{locals()[param]}'"})

        # add in the new workbench retrieval flag
        kwargs.update({'use_new_wb': use_new_wb})

        # add in the new workbench retrieval flag
        kwargs.update({'use_v3_sp': use_v3_sp})

        # try to make the call for records
        ret_val = db_info.get_map_catalog_data(**kwargs)

        # check the return for any detected errors or warnings
        if 'Error' in ret_val:
            # set the status to a server error
            status_code = 500
        # elif 'Warning' in ret_val:
        #     # set the status to a not found
        #     status_code = 404
        else:
            # if there was DB error
            if ret_val == -1:
                ret_val = {'Error': 'Database error getting catalog member data.'}

                # set the status to a not found
                status_code = 500
            # check the return, no data gets a 404 return
            elif 'catalog' not in ret_val:
                # set a warning message
                ret_val = {'Warning': 'No data found using the filter criteria selected.'}

    except Exception:
        # return a failure message
        ret_val = {'Exception': 'Error detected trying to get the map catalog data.'}

        # log the exception
        logger.exception(ret_val)

        # set the status to a server error
        status_code = 500

    # return to the caller
    return JSONResponse(content=ret_val, status_code=status_code, media_type="application/json")


@APP.get('/get_ui_instance_name', dependencies=[Depends(JWTBearer(security))], status_code=200, response_model=None)
async def get_ui_instance_name(site_branding: BrandName, instance_name: Union[str, None] = Query(default=None), reset: Union[bool, None] = Query(default=False),
                                    ) -> PlainTextResponse:
    """
    Gets the default instance name value
    <br/>Note: Valid instance names are less than 30 characters in length
        <br/>&nbsp;&nbsp;&nbsp;site_branding: The branding of the website.
        <br/>&nbsp;&nbsp;&nbsp;instance_name: The new instance name to be used
        <br/>&nbsp;&nbsp;&nbsp;reset: Flag to remove the instance name from storage
    """
    # init the returns
    ret_val: str = ''
    status_code: int = 200

    try:
        # set the default file path
        file_path: str = os.getenv('INSTANCE_NAME_FILE_PATH', '') + site_branding

        # was the config file path found
        if file_path is not None:
            # if this was a reset or a data set request
            if reset and os.path.exists(file_path):
                # delete the config file
                os.remove(file_path)

            # set the instance name if it came in
            elif instance_name is not None:
                # if the instance_name too long
                if len(instance_name) > 30:
                    ret_val = 'Error: Instance name > 30 characters.'
                else:
                    # open the config file for writing
                    with open(file_path, 'w') as fp:
                        fp.write(instance_name)

                        # save the instance name
                        ret_val = instance_name

            elif instance_name is None and os.path.exists(file_path):
                # open the config file for reading
                with open(file_path, 'r') as fp:
                    # save the instance name in the file
                    ret_val = fp.read()

                    # if we encountered an empty file
                    if ret_val == '':
                        ret_val = "Error: No value found in storage."
            else:
                ret_val = "Error: No saved value found."
        else:
            logger.error('Error: Instance name file path not set.')

    except Exception as e:
        # log the issue
        logger.exception(e)

        # set the error return
        ret_val = 'Error detected.'

    # return to the caller
    return PlainTextResponse(content=ret_val, status_code=status_code, media_type="text/plain")


@APP.get('/get_catalog_workbench', dependencies=[Depends(JWTBearer(security))], status_code=200, response_model=None)
async def get_catalog_workbench(insertion_date: Union[str, None] = Query(default=None), met_class: Union[str, None] = Query(default=None),
                                physical_location: Union[str, None] = Query(default=None), instance_name: Union[str, None] = Query(default=None),
                                ensemble_name: Union[str, None] = Query(default=None),project_code: Union[str, None] = Query(default=None),
                                max_age: int = Query(default=1)) -> json:
    """
    Gets the latest workbench
    <br/>Note: Leave filtering params empty if not desired.

    <br/>&nbsp;&nbsp;&nbsp;insertion_date: The timestamp the run props were inserted into the database
    <br/>&nbsp;&nbsp;&nbsp;met_class: The meteorological class (synoptic or tropical)
    <br/>&nbsp;&nbsp;&nbsp;physical_location: The site that made the request (RENCI, PSC, etc.)
    <br/>&nbsp;&nbsp;&nbsp;instance_name: Filter by the name of the ECFLOW instance name
    <br/>&nbsp;&nbsp;&nbsp;ensemble_name: The type of run (gfsforecast, nowcast, ofcl, etc.)
    <br/>&nbsp;&nbsp;&nbsp;project_code: The requesting project code (ecflow_test_renci, ncf, nopp, test, unc-crc, etc.)
    <br/>&nbsp;&nbsp;&nbsp;max_age: The maximum age of a tropical run in days.

    :return:
    """
    # init the returned data and html status code
    ret_val: dict = {}
    status_code: int = 200

    try:
        logger.debug('Input params - insertion_date: %s, met_class: %s, physical_location: %s, instance_name: %s, ensemble_name: %s, project_code: '
                     '%s, max_age: %s',
                     insertion_date, met_class, physical_location, instance_name, ensemble_name, project_code, max_age)

        # init the kwargs variable
        kwargs: dict = {}

        # create the param list
        params: list = ['insertion_date', 'met_class', 'physical_location', 'instance_name', 'ensemble_name', 'project_code']

        # loop through the SP params passed in
        for param in params:
            # add this parm to the list
            kwargs.update({param: 'null' if not locals()[param] else f"'{locals()[param]}'"})

        # add in the max age int
        kwargs.update({'max_age': max_age})

        # try to make the call for records
        ret_val = db_info.get_map_workbench_data(**kwargs)

        # check the return
        if ret_val == -1:
            ret_val = {'Error': 'Database error getting catalog workbench data.'}

            # set the status to a not found
            status_code = 500
        # check the return, no data gets a 404 return
        elif len(ret_val) == 0:
            # set a warning message
            ret_val = {'Warning': 'No data found using the filter criteria selected.'}

    except Exception:
        # return a failure message
        ret_val = {'Exception': 'Error detected trying to get the map catalog workbench data.'}

        # log the exception
        logger.exception(ret_val)

        # set the status to a server error
        status_code = 500

    # return to the caller
    return JSONResponse(content=ret_val, status_code=status_code, media_type="application/json")


@APP.get('/get_ui_data_secure', dependencies=[Depends(JWTBearer(security))], status_code=200, response_model=None)
async def get_ui_data_secure(run_id: Union[str, None] = Query(default=None), grid_type: Union[str, None] = Query(default=None),
                                             event_type: Union[str, None] = Query(default=None),
                                             instance_name: Union[str, None] = Query(default=None), met_class: Union[str, None] = Query(default=None),
                                             storm_name: Union[str, None] = Query(default=None), cycle: Union[str, None] = Query(default=None),
                                             advisory_number: Union[str, None] = Query(default=None),
                                             run_date: Union[str, None] = Query(default=None), end_date: Union[str, None] = Query(default=None),
                                             project_code: Union[str, None] = Query(default=None),
                                             ensemble_name: Union[str, None] = Query(default=None),
                                             product_type: Union[str, None] = Query(default=None), limit: Union[int, None] = Query(default=7),
                                             use_new_wb: Union[bool, None] = Query(default=False),
                                             use_v3_sp: Union[bool, None] = Query(default=False), ) -> json:
    """
    Gets the json formatted map UI catalog data.
    4460-2024020500-gfsforecast
    <br/>Note: Leave filtering params empty if not desired.
    <br/>&nbsp;&nbsp;&nbsp;run_id: Filter by the run ID
    <br/>&nbsp;&nbsp;&nbsp;grid_type: Filter by the name of the grid
    <br/>&nbsp;&nbsp;&nbsp;event_type: Filter by the event type
    <br/>&nbsp;&nbsp;&nbsp;instance_name: Filter by the name of the ECFLOW instance
    <br/>&nbsp;&nbsp;&nbsp;met_class: Filter by the meteorological class
    <br/>&nbsp;&nbsp;&nbsp;storm_name: Filter by the storm name
    <br/>&nbsp;&nbsp;&nbsp;cycle: Filter by the cycle
    <br/>&nbsp;&nbsp;&nbsp;advisory_number: Filter by the advisory number
    <br/>&nbsp;&nbsp;&nbsp;run_date: Filter by the run date in the form of yyyy-mm-dd
    <br/>&nbsp;&nbsp;&nbsp;end_date: Filter by the data between the run date and end date
    <br/>&nbsp;&nbsp;&nbsp;project_code: Filter by the project code
    <br/>&nbsp;&nbsp;&nbsp;ensemble_name: The name of the run ensemble
    <br/>&nbsp;&nbsp;&nbsp;product_type: Filter by the product type
    <br/>&nbsp;&nbsp;&nbsp;limit: Limit the number of catalog records returned in days (default is 7)
    <br/>&nbsp;&nbsp;&nbsp;use_new_wb: Use the new catalog workbench code
    <br/>&nbsp;&nbsp;&nbsp;use_v3_sp: Use the UI v3 data stored procedure
    """
    # init the returned data and html status code
    ret_val: dict = {}
    status_code: int = 200

    try:
        logger.debug(
            'Params - run_id: %s, grid_type: %s, event_type: %s, instance_name: %s, met_class: %s, storm_name: %s, cycle: %s, advisory_number: %s, '
            'run_date: %s, end_date: %s, project_code %s, product_type: %s, limit: %s, ensemble_name: %s', run_id, grid_type, event_type,
            instance_name, met_class, storm_name, cycle, advisory_number, run_date, end_date, project_code, product_type, limit, ensemble_name)

        # init the kwargs variable
        kwargs: dict = {}

        # create the param list
        params: list = ['run_id', 'grid_type', 'event_type', 'instance_name', 'met_class', 'storm_name', 'cycle', 'advisory_number', 'run_date',
                        'end_date', 'project_code', 'product_type', 'limit', 'ensemble_name']

        # loop through the SP params passed in
        for param in params:
            # add this parm to the list
            kwargs.update({param: 'null' if not locals()[param] else f"'{locals()[param]}'"})

        # add in the new workbench retrieval flag
        kwargs.update({'use_new_wb': use_new_wb})

        # add in the new workbench retrieval flag
        kwargs.update({'use_v3_sp': use_v3_sp})

        # if there was a run id specified make it a wildcard
        if run_id is not None:
            # add in the new workbench retrieval flag
            # kwargs.update({'use_new_wb': False})
            kwargs.update({'run_id': run_id + '%'})

        # try to make the call for records
        ret_val = db_info.get_map_catalog_data(**kwargs)

        # check the return
        if ret_val == -1:
            ret_val = {'Error': 'Database error getting catalog member data.'}

            # set the status to a not found
            status_code = 500
        # check the return, no data gets a 404 return
        elif 'catalog' not in ret_val:
            # set a warning message
            ret_val = {'Warning': 'No data found using the filter criteria selected.'}

    except Exception:
        # return a failure message
        ret_val = {'Exception': 'Error detected trying to get the map catalog data.'}

        # log the exception
        logger.exception(ret_val)

        # set the status to a server error
        status_code = 500

    # return to the caller
    return JSONResponse(content=ret_val, status_code=status_code, media_type="application/json")


@APP.get('/get_ui_data_file', status_code=200, response_model=None)
async def get_ui_data_file(file_name: Union[str, None] = Query(default='apsviz.json'),
                                           grid_type: Union[str, None] = Query(default=None), event_type: Union[str, None] = Query(default=None),
                                           instance_name: Union[str, None] = Query(default=None), met_class: Union[str, None] = Query(default=None),
                                           storm_name: Union[str, None] = Query(default=None), cycle: Union[str, None] = Query(default=None),
                                           advisory_number: Union[str, None] = Query(default=None), run_date: Union[str, None] = Query(default=None),
                                           end_date: Union[str, None] = Query(default=None), project_code: Union[str, None] = Query(default=None),
                                           ensemble_name: Union[str, None] = Query(default=None),
                                           product_type: Union[str, None] = Query(default=None), limit: Union[int, None] = Query(default=7),
                                           use_new_wb: Union[bool, None] = Query(default=False),
                                           use_v3_sp: Union[bool, None] = Query(default=False), ) -> json:
    """
    Returns the json formatted map UI catalog data in a file specified.
    <br/>Note: Leave filtering params empty if not desired.
    <br/>&nbsp;&nbsp;&nbsp;file_name: The name of the output file (default is apsviz.json)
    <br/>&nbsp;&nbsp;&nbsp;grid_type: Filter by the name of the grid
    <br/>&nbsp;&nbsp;&nbsp;event_type: Filter by the event type
    <br/>&nbsp;&nbsp;&nbsp;instance_name: Filter by the name of the ECFLOW instance
    <br/>&nbsp;&nbsp;&nbsp;met_class: Filter by the meteorological class
    <br/>&nbsp;&nbsp;&nbsp;storm_name: Filter by the storm name
    <br/>&nbsp;&nbsp;&nbsp;cycle: Filter by the cycle
    <br/>&nbsp;&nbsp;&nbsp;advisory_number: Filter by the advisory number
    <br/>&nbsp;&nbsp;&nbsp;run_date: Filter by the run date in the form of yyyy-mm-dd
    <br/>&nbsp;&nbsp;&nbsp;end_date: Filter by the data between the run date and end date
    <br/>&nbsp;&nbsp;&nbsp;project_code: Filter by the project code
    <br/>&nbsp;&nbsp;&nbsp;ensemble_name: The name of the run ensemble
    <br/>&nbsp;&nbsp;&nbsp;product_type: Filter by the product type
    <br/>&nbsp;&nbsp;&nbsp;limit: Limit the number of catalog records returned in days (default is 7)
    <br/>&nbsp;&nbsp;&nbsp;use_new_wb: Use the new catalog workbench code
    <br/>&nbsp;&nbsp;&nbsp;use_v3_sp: Use the new v3 data stored procedure
    """
    # pylint: disable=unused-argument
    # pylint: disable=too-many-arguments
    # pylint: disable=too-many-locals

    # init the returned html status code
    status_code: int = 200

    # init the kwargs variable
    kwargs: dict = {}

    # create the param list
    params: list = ['grid_type', 'event_type', 'instance_name', 'met_class', 'storm_name', 'cycle', 'advisory_number', 'run_date', 'end_date',
                    'project_code', 'product_type', 'limit', 'ensemble_name']

    # loop through the SP params passed in
    for param in params:
        # add this parm to the list
        kwargs.update({param: 'null' if not locals()[param] else f"'{locals()[param]}'"})

    # add in the new workbench retrieval flag
    kwargs.update({'use_new_wb': use_new_wb})

    # add in the new workbench retrieval flag
    kwargs.update({'use_v3_sp': use_v3_sp})

    # get a file path to the temp file directory.
    # append a unique path to avoid collisions
    temp_file_path: str = os.path.join(os.getenv('TEMP_FILE_PATH', os.path.dirname(__file__)), str(uuid.uuid4()))

    # append the file name
    file_path: str = os.path.join(temp_file_path, file_name)

    try:
        # try to make the call for records
        ret_val: dict = db_info.get_map_catalog_data(**kwargs)

        # check the return for any detected errors or warnings
        if 'Error' in ret_val:
            # set the returned status code
            status_code = 500
        elif 'Warning' in ret_val:
            # set the returned status code
            status_code = 400
        else:
            # was there a DB error
            if ret_val == -1:
                # set a error message
                ret_val = {'Error': 'Database error getting catalog member data.'}

                # set the status to a not found
                status_code = 500
            # check the return, no data gets a 404 return
            elif 'catalog' not in ret_val:
                # set a warning message
                ret_val = {'Warning': 'No data found using the filter criteria selected.'}

            else:
                # make the directory
                os.makedirs(temp_file_path)

                # write out the data to a file
                with open(file_path, 'w', encoding='utf-8') as f_h:
                    json.dump(ret_val, f_h)

    except Exception:
        # log the exception
        logger.exception('Exception detected on UI data file request.')

        # set the status to a server error
        status_code = 500

    # return to the caller
    return FileResponse(path=file_path, filename=file_name, media_type='text/json', status_code=status_code,
                        background=BackgroundTask(GenUtils.cleanup, temp_file_path))


@APP.get('/get_geo_point_data', status_code=200, response_model=None, response_class=PlainTextResponse)
def get_geo_point_data(lon: Union[float, None] = Query(default=None), lat: Union[float, None] = Query(default=None),
                       variable_name: Union[str, None] = Query(default=None), kmax: Union[str, None] = Query(default='10'),
                       alt_urlsource: Union[str, None] = Query(default=None), url: Union[str, None] = Query(default=None),
                       keep_headers: Union[bool, None] = Query(default=True), ensemble: Union[str, None] = Query(default=None),
                       ndays: Union[str, None] = Query(default='0'), tds_svr: Union[str, None] = Query(default=None)) -> PlainTextResponse:
    """
    Returns the CSV formatted geo point data

    Note that all fields are mandatory.

    :return:
    """
    # init the return and html status code
    ret_val: str = ''
    status_code: int = 200

    logger.debug(
        'Input params - lon: %s, lat: %s, variable_name: %s, kmax: %s, alt_urlsource: %s, url: %s, keep_headers: %s, ensemble: %s, ndays: %s, tds_svr: %s', lon,
        lat, variable_name, kmax, alt_urlsource, url, keep_headers, ensemble, ndays, tds_svr)

    try:
        # validate the input. these are not optional
        if lat or lon or kmax or url or ndays or tds_svr:
            # init the kwargs variable
            kwargs: dict = {}

            # create the param list
            params: list = ['lat', 'lon', 'variable_name', 'kmax', 'alt_urlsource', 'url', 'keep_headers', 'ensemble', 'ndays', 'tds_svr']

            # loop through the SP params passed in
            for param in params:
                # add this parm to the list
                kwargs.update({param: None if not locals()[param] else f'{locals()[param]}'})

            # make the call to get the geo point data
            gp = GeoPoint(logger)

            # try to make the call for records
            ret_val: str = gp.get_geo_point_data(**kwargs)

            # if the call was successful
            if len(ret_val) == 0:
                # set the Warning message and the return status
                ret_val = 'Warning: No geo point data found at that point.'
        else:
            # set the error message
            ret_val = 'Error Invalid input. Insure that all input fields are populated.'

    except Exception:
        # return a failure message
        ret_val = 'Exception detected trying to get geo point data.'

        # log the exception
        logger.exception(ret_val)

        # set the status to a server error
        status_code = 500

    # return to the caller
    return PlainTextResponse(content=ret_val, status_code=status_code, media_type="text/csv")


@APP.get('/get_station_data', status_code=200, response_model=None, response_class=PlainTextResponse)
def get_station_data(station_name: Union[str, None] = Query(default=None), time_mark: Union[str, None] = Query(default=None),
                     data_source: Union[str, None] = Query(default=None), instance_name: Union[str, None] = Query(default=None),
                     forcing_metclass: Union[str, None] = Query(default=None)) -> PlainTextResponse:
    """
    Returns the CSV formatted observational station.

    Note that all fields are mandatory.

    :return:
    """
    # init the return and html status code
    ret_val: str = ''
    status_code: int = 200

    # example input - station name: 8651370, timemark: 2023-08-24T00:00:00, data_source: GFSFORECAST_WNAT_53K_V1.0
    try:
        # validate the input. nothing is optional
        if station_name or time_mark or data_source or instance_name or forcing_metclass:
            # init the kwargs variable
            kwargs: dict = {}

            # create the param list
            params: list = ['station_name', 'time_mark', 'data_source', 'instance_name', 'forcing_metclass']

            # loop through the SP params passed in
            for param in params:
                # add this parm to the list
                kwargs.update({param: 'null' if not locals()[param] else f'{locals()[param]}'})

            # try to make the call for records
            ret_val: str = db_info.get_station_data(**kwargs)

            # was the call successful
            if len(ret_val) == 0:
                # set the Warning message and the return status
                ret_val = 'Warning: No station data found using the criteria selected.'
        else:
            # set the error message
            ret_val = 'Error Invalid input. Insure that all input fields are populated.'

    except Exception:
        # return a failure message
        ret_val = 'Exception detected trying to get station data.'

        # log the exception
        logger.exception(ret_val)

        # set the status to a server error
        status_code = 500

    # return to the caller
    return PlainTextResponse(content=ret_val, status_code=status_code, media_type="text/csv")


@APP.get('/get_station_data_file', status_code=200, response_model=None)
async def get_station_data_file(file_name: Union[str, None] = Query(default='station.csv'), station_name: Union[str, None] = Query(default=None),
                                time_mark: Union[str, None] = Query(default=None), data_source: Union[str, None] = Query(default=None),
                                instance_name: Union[str, None] = Query(default=None),
                                forcing_metclass: Union[str, None] = Query(default=None)) -> csv:
    """
    Returns the CSV formatted observational station data as a csv file.

    Note that all fields are mandatory.

    :return:
    """
    # init the return and html status code
    ret_val: str = ''
    status_code: int = 200

    # get a file path to the temp file directory.
    # append a unique path to avoid collisions
    temp_file_path: str = os.path.join(os.getenv('TEMP_FILE_PATH', os.path.dirname(__file__)), str(uuid.uuid4()))

    # append the file name
    file_path: str = os.path.join(temp_file_path, file_name)

    # example input - station name: 8728690,
    #                 timemark: 2024-03-07T00:00:00Z,
    #                 data_source: GFSFORECAST_NCSC_SAB_V1.23
    #                 instance_name: ncsc123_gfs_sb55.01
    #                 forcing_metclass: synoptic

    try:
        # validate the input. nothing is optional
        if station_name or time_mark or data_source or instance_name or forcing_metclass:
            # init the kwargs variable
            kwargs: dict = {}

            # create the param list
            params: list = ['station_name', 'time_mark', 'data_source', 'instance_name', 'forcing_metclass']

            # loop through the SP params passed in
            for param in params:
                # add this parm to the list
                kwargs.update({param: 'null' if not locals()[param] else f'{locals()[param]}'})

            # try to make the call for records
            ret_val: str = db_info.get_station_data(**kwargs)

            # was the call successful
            if len(ret_val) == 0:
                # set the Warning message and the return status
                ret_val = 'Warning: No station data found using the criteria selected.'

            else:
                # make the directory
                os.makedirs(temp_file_path)

                # write out the data to a file
                reader = csv.reader(ret_val.splitlines(), skipinitialspace=True)
                with open(file_path, 'w', encoding="utf-8") as f_h:
                    writer = csv.writer(f_h)
                    writer.writerows(reader)
        else:
            # set the error message
            ret_val = 'Error Invalid input. Insure that all input fields are populated.'

    except Exception:
        # return a failure message
        ret_val = 'Exception detected trying to get station data.'

        # log the exception
        logger.exception(ret_val)

        # set the status to a server error
        status_code = 500

    # return to the caller
    return FileResponse(path=file_path, filename=file_name, media_type='text/csv', status_code=status_code,
                        background=BackgroundTask(GenUtils.cleanup, temp_file_path))


@APP.get('/get_catalog_member_records', status_code=200, response_model=None)
async def get_catalog_member_records(run_id: Union[str, None] = Query(default=None), project_code: Union[str, None] = Query(default=None),
                                     filter_event_type: Union[str, None] = Query(default=None), limit: Union[int, None] = Query(default=4)) -> json:
    """
    Gets the json formatted catalog member data.
    <br/>Note: Leave filtering params empty if not desired.
    <br/>&nbsp;&nbsp;&nbsp;run_id: Filter by the name of the ECFLOW grid. Leaving this empty will result in getting the latest <limit> records.
    <br/>&nbsp;&nbsp;&nbsp;project_code: Filter by the project code.
    <br/>&nbsp;&nbsp;&nbsp;filter_event_type: Filter out records by event type.
    <br/>&nbsp;&nbsp;&nbsp;limit: limit the number of records returned. only applicable when run_id is empty.
    """
    # init the returned data and html status code
    ret_val: dict = {}
    status_code: int = 200

    try:
        logger.debug('Input params - run_id: %s, project_code: %s, filter_event_type: %s, limit: %s', run_id, project_code, filter_event_type, limit)

        # init the kwargs variable
        kwargs: dict = {}

        # create the param list
        params: list = ['run_id', 'project_code', 'filter_event_type', 'limit']

        # if we get a run id add on a wildcard for the search
        if run_id is not None:
            run_id += '%'

        # loop through the SP params passed in
        for param in params:
            # add this parm to the list
            kwargs.update({param: 'null' if not locals()[param] else f"'{locals()[param]}'"})

        # try to make the call for records
        ret_val: dict = db_info.get_catalog_member_records(**kwargs)

        # check the return
        if ret_val == -1:
            ret_val = {'Error': 'Database error getting catalog member data.'}

            # set the status to a server error
            status_code = 500
        else:
            # did we get everything expected
            if ret_val is not None and ret_val['catalogs'] is not None:
                # remove non-PSC catalog items
                ret_val = GenUtils.filter_catalog_past_runs(ret_val)
            else:
                # return a failure message
                ret_val = {'Warning': 'No data found using the filter criteria selected.'}

    except Exception:
        # return a failure message
        ret_val = {'Error': 'Exception detected trying to get the catalog member data.'}

        # log the exception
        logger.exception(ret_val)

        # set the status to a server error
        status_code = 500

    # return to the caller
    return JSONResponse(content=ret_val, status_code=status_code, media_type="application/json")


@APP.get('/get_pulldown_data', status_code=200, response_model=None)
async def get_pulldown_data(grid_type: Union[str, None] = Query(default=None), event_type: Union[str, None] = Query(default=None),
                            instance_name: Union[str, None] = Query(default=None), met_class: Union[str, None] = Query(default=None),
                            storm_name: Union[str, None] = Query(default=None), cycle: Union[str, None] = Query(default=None),
                            advisory_number: Union[str, None] = Query(default=None), run_date: Union[str, None] = Query(default=None),
                            end_date: Union[str, None] = Query(default=None), project_code: Union[str, None] = Query(default=None),
                            product_type: Union[str, None] = Query(default=None), psc_output: bool = False) -> json:
    """
    Gets the json formatted UI pulldown data.
    <br/>Note: Leave filtering params empty if not desired.
    <br/>&nbsp;&nbsp;&nbsp;grid_type: Filter by the name of the ECFLOW grid
    <br/>&nbsp;&nbsp;&nbsp;event_type: Filter by the event type
    <br/>&nbsp;&nbsp;&nbsp;instance_name: Filter by the name of the ECFLOW instance
    <br/>&nbsp;&nbsp;&nbsp;met_class: Filter by the meteorological class
    <br/>&nbsp;&nbsp;&nbsp;storm_name: Filter by the storm name
    <br/>&nbsp;&nbsp;&nbsp;cycle: Filter by the cycle
    <br/>&nbsp;&nbsp;&nbsp;advisory_number: Filter by the advisory number
    <br/>&nbsp;&nbsp;&nbsp;run_date: Filter by the run date in the form of yyyy-mm-dd
    <br/>&nbsp;&nbsp;&nbsp;end_date: Filter by the data between the run date and end date
    <br/>&nbsp;&nbsp;&nbsp;project_code: Filter by the project code
    <br/>&nbsp;&nbsp;&nbsp;product_type: Filter by the product type
    <br/>&nbsp;&nbsp;&nbsp;psc_output: True if PSC output format is desired
    """
    # init the returned data and html status code
    ret_val: dict = {}
    status_code: int = 200

    try:
        logger.debug(
            'Input params - grid_type: %s, event_type: %s, instance_name: %s, met_class: %s, storm_name: %s, cycle: %s, advisory_number: %s, '
            'run_date: %s, end_date: %s, project_code %s, product_type: %s', grid_type, event_type, instance_name, met_class, storm_name, cycle,
            advisory_number, run_date, end_date, project_code, product_type)

        # init the kwargs variable
        kwargs: dict = {}

        # create the param list
        params: list = ['grid_type', 'event_type', 'instance_name', 'met_class', 'storm_name', 'cycle', 'advisory_number', 'run_date', 'end_date',
                        'project_code', 'product_type']

        # loop through the SP params passed in
        for param in params:
            # add this parm to the list
            kwargs.update({param: 'null' if not locals()[param] else f"'{locals()[param]}'"})

        # try to make the call for records
        ret_val: dict = db_info.get_pull_down_data(**kwargs)

        # check the return
        if ret_val == -1:
            ret_val = {'Error': 'Database error getting pulldown data.'}

            # set the status to a server error
            status_code = 500
        # if PSC output is requested
        elif psc_output:
            # collect the choices
            choices_data: dict = {'model': ['nhc', 'gfs'], 'storm': ret_val['storm_names'], 'mesh': ret_val['grid_types'],
                                  'advisory': ret_val['advisory_numbers'], 'ensembleMember': ret_val['event_types'],
                                  'metric': ret_val['product_types'], 'cycle': ret_val['cycles'], 'datetime': ret_val['run_dates']}

            # create a new dict for return
            ret_val = {'choices': choices_data}

    except Exception:
        # return a failure message
        ret_val = {'Error': 'Exception detected trying to get the UI pulldown data.'}

        # log the exception
        logger.exception(ret_val)

        # set the status to a server error
        status_code = 500

    # return to the caller
    return JSONResponse(content=ret_val, status_code=status_code, media_type="application/json")
