# SPDX-FileCopyrightText: 2022 Renaissance Computing Institute. All rights reserved.
# SPDX-FileCopyrightText: 2023 Renaissance Computing Institute. All rights reserved.
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

from typing import Union

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, FileResponse, PlainTextResponse

from src.common.logger import LoggingUtil
from src.common.pg_impl import PGImplementation

# set the app version
app_version = os.getenv('APP_VERSION', 'Version number not set')

# declare the FastAPI details
APP = FastAPI(title='APSVIZ UI Data', version=app_version)

# get the log level and directory from the environment.
log_level, log_path = LoggingUtil.prep_for_logging()

# create a logger
logger = LoggingUtil.init_logging("APSVIZ.ui-data.ui", level=log_level, line_format='medium', log_file_path=log_path)

# declare app access details
APP.add_middleware(CORSMiddleware, allow_origins=['*'], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

# declare the database to use
db_name: tuple = ('apsviz', 'apsviz_gauges', 'asgs')

# create a DB connection object
db_info: PGImplementation = PGImplementation(db_name, _logger=logger)


@APP.get('/get_ui_data', status_code=200, response_model=None)
async def get_terria_map_catalog_data(grid_type: Union[str, None] = Query(default=None), event_type: Union[str, None] = Query(default=None),
                                      instance_name: Union[str, None] = Query(default=None), met_class: Union[str, None] = Query(default=None),
                                      storm_name: Union[str, None] = Query(default=None), cycle: Union[str, None] = Query(default=None),
                                      advisory_number: Union[str, None] = Query(default=None), run_date: Union[str, None] = Query(default=None),
                                      end_date: Union[str, None] = Query(default=None), project_code: Union[str, None] = Query(default=None),
                                      product_type: Union[str, None] = Query(default=None), limit: Union[int, None] = Query(default=4)) -> json:
    """
    Gets the json formatted terria map UI catalog data.
    <br/>Note: Leave filtering params empty if not desired.
    <br/>&nbsp;&nbsp;&nbsp;grid_type: Filter by the name of the ASGS grid
    <br/>&nbsp;&nbsp;&nbsp;event_type: Filter by the event type
    <br/>&nbsp;&nbsp;&nbsp;instance_name: Filter by the name of the ASGS instance
    <br/>&nbsp;&nbsp;&nbsp;met_class: Filter by the meteorological class
    <br/>&nbsp;&nbsp;&nbsp;storm_name: Filter by the storm name
    <br/>&nbsp;&nbsp;&nbsp;cycle: Filter by the cycle
    <br/>&nbsp;&nbsp;&nbsp;advisory_number: Filter by the advisory number
    <br/>&nbsp;&nbsp;&nbsp;run_date: Filter by the run date in the form of yyyy-mm-dd
    <br/>&nbsp;&nbsp;&nbsp;end_date: Filter by the data between the run date and end date
    <br/>&nbsp;&nbsp;&nbsp;project_code: Filter by the project code
    <br/>&nbsp;&nbsp;&nbsp;product_type: Filter by the product type
    <br/>&nbsp;&nbsp;&nbsp;limit: Limit the number of catalog records returned (default is 4)
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
                        'project_code', 'product_type', 'limit']

        # loop through the SP params passed in
        for param in params:
            # add this parm to the list
            kwargs.update({param: 'null' if not locals()[param] else f"'{locals()[param]}'"})

        # try to make the call for records
        ret_val = db_info.get_terria_map_catalog_data(**kwargs)

        # check the return, no data gets a 404 return
        if ret_val['catalog'] is None:
            # set a warning message
            ret_val = {'Warning': 'No data found using the filter criteria selected.'}

            # set the status to a not found
            status_code = 404

    except Exception:
        # return a failure message
        ret_val = {'Exception': 'Error detected trying to get the terria map catalog data.'}

        # log the exception
        logger.exception(ret_val)

        # set the status to a server error
        status_code = 500

    # return to the caller
    return JSONResponse(content=ret_val, status_code=status_code, media_type="application/json")


@APP.get('/get_ui_data_file', status_code=200, response_model=None)
async def get_terria_map_catalog_data_file(file_name: Union[str, None] = Query(default='apsviz.json'),
                                           grid_type: Union[str, None] = Query(default=None), event_type: Union[str, None] = Query(default=None),
                                           instance_name: Union[str, None] = Query(default=None), met_class: Union[str, None] = Query(default=None),
                                           storm_name: Union[str, None] = Query(default=None), cycle: Union[str, None] = Query(default=None),
                                           advisory_number: Union[str, None] = Query(default=None), run_date: Union[str, None] = Query(default=None),
                                           end_date: Union[str, None] = Query(default=None), project_code: Union[str, None] = Query(default=None),
                                           product_type: Union[str, None] = Query(default=None), limit: Union[int, None] = Query(default=4)) -> json:
    """
    Returns the json formatted terria map UI catalog data in a file specified.
    <br/>Note: Leave filtering params empty if not desired.
    <br/>&nbsp;&nbsp;&nbsp;file_name: The name of the output file (default is apsviz.json)
    <br/>&nbsp;&nbsp;&nbsp;grid_type: Filter by the name of the ASGS grid
    <br/>&nbsp;&nbsp;&nbsp;event_type: Filter by the event type
    <br/>&nbsp;&nbsp;&nbsp;instance_name: Filter by the name of the ASGS instance
    <br/>&nbsp;&nbsp;&nbsp;met_class: Filter by the meteorological class
    <br/>&nbsp;&nbsp;&nbsp;storm_name: Filter by the storm name
    <br/>&nbsp;&nbsp;&nbsp;cycle: Filter by the cycle
    <br/>&nbsp;&nbsp;&nbsp;advisory_number: Filter by the advisory number
    <br/>&nbsp;&nbsp;&nbsp;run_date: Filter by the run date in the form of yyyy-mm-dd
    <br/>&nbsp;&nbsp;&nbsp;end_date: Filter by the data between the run date and end date
    <br/>&nbsp;&nbsp;&nbsp;project_code: Filter by the project code
    <br/>&nbsp;&nbsp;&nbsp;product_type: Filter by the product type
    <br/>&nbsp;&nbsp;&nbsp;limit: Limit the number of catalog records returned (default is 4)
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
                    'project_code', 'product_type', 'limit']

    # loop through the SP params passed in
    for param in params:
        # add this parm to the list
        kwargs.update({param: 'null' if not locals()[param] else f"'{locals()[param]}'"})

    # get a file path to the temp file directory.
    # append a unique path to avoid collisions
    temp_file_path: str = os.path.join(os.getenv('TEMP_FILE_PATH', os.path.dirname(__file__)), str(uuid.uuid4()))

    # make the directory
    os.makedirs(temp_file_path)

    # append the file name
    temp_file_path: str = os.path.join(temp_file_path, file_name)

    try:
        # try to make the call for records
        ret_val: dict = db_info.get_terria_map_catalog_data(**kwargs)

        # check the return, no catalog data gets not found warning
        if ret_val['catalog'] is None :
            # set a warning message
            ret_val = {'Warning': 'No data found using the filter criteria selected.'}

            # set the status to a not found
            status_code = 404

        # write out the data to a file
        with open(temp_file_path, 'w', encoding='utf-8') as f_h:
            json.dump(ret_val, f_h)

    except Exception:
        # log the exception
        logger.exception('Exception detected on UI data file request.')

        # set the status to a server error
        status_code = 500

    # return to the caller
    return FileResponse(path=temp_file_path, filename=file_name, media_type='text/json', status_code=status_code)


@APP.get('/get_obs_station_data', status_code=200, response_model=None, response_class=PlainTextResponse)
def get_obs_station_data(station_name: Union[str, None] = Query(default=None), start_date: Union[str, None] = Query(default=None),
                         end_date: Union[str, None] = Query(default=None)) -> PlainTextResponse:
    """
    Returns the CSV formatted observational station.

    Note that all fields are mandatory.

    :return:
    """
    # init the return and html status code
    ret_val: str = ''
    status_code: int = 200

    try:
        # validate the input. nothing is optional
        if station_name or start_date or end_date:
            # init the kwargs variable
            kwargs: dict = {}

            # create the param list
            params: list = ['station_name', 'start_date', 'end_date']

            # loop through the SP params passed in
            for param in params:
                # add this parm to the list
                kwargs.update({param: 'null' if not locals()[param] else f"'{locals()[param]}'"})

            # try to make the call for records
            station_data: dict = db_info.get_obs_station_data(**kwargs)

            # was the call successful
            if station_data != -1:
                # start getting the CSV data. first get the columns
                csv_data = ','.join(list(station_data[0].keys())) + '\n'

                # now get the data
                for item in station_data:
                    # convert it into CSV
                    csv_data += ','.join([str(x) if x is not None else '' for x in list(item.values())]) + '\n'

                logger.debug('Output data: %s', csv_data)

                # set the return data
                ret_val = csv_data
            else:
                # set the Warning message and the return status
                ret_val = 'Warning: No data found using the filter criteria selected.'

                # set the status to a not found
                status_code = 404
        else:
            # set the error message
            ret_val = 'Error Invalid input. Insure that all input fields are populated.'

            # set the status to a not found
            status_code = 404

    except Exception:
        # return a failure message
        ret_val = 'Exception detected trying to get station data.'

        # log the exception
        logger.exception(ret_val)

        # set the status to a server error
        status_code = 500

    # return to the caller
    return PlainTextResponse(content=ret_val, status_code=status_code, media_type="text/plain")


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
    <br/>&nbsp;&nbsp;&nbsp;grid_type: Filter by the name of the ASGS grid
    <br/>&nbsp;&nbsp;&nbsp;event_type: Filter by the event type
    <br/>&nbsp;&nbsp;&nbsp;instance_name: Filter by the name of the ASGS instance
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
            ret_val = {'Warning': 'No data found using the filter criteria selected.'}
        # if PSC output is requested
        elif psc_output:
            # collect the choices
            choices_data: dict = {'model': ['nhc', 'gfs'], 'storm': ret_val['storm_names'],
                                  'mesh': ret_val['grid_types'], 'advisory': ret_val['advisory_numbers'], 'ensembleMember': ret_val['event_types'],
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
