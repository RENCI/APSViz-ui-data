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
from fastapi.responses import JSONResponse, FileResponse

from common.logger import LoggingUtil
from common.pg_utils import PGUtils

# set the app version
APP_VERSION = 'v0.2.6'

# get the DB connection details for the apsviz DB
apsviz_dbname = os.environ.get('APSVIZ_DB_DATABASE')
apsviz_username = os.environ.get('APSVIZ_DB_USERNAME')
apsviz_password = os.environ.get('APSVIZ_DB_PASSWORD')

# create a logger
logger = LoggingUtil.init_logging("APSVIZ.ui-data.ui", line_format='medium')

# declare the FastAPI details
APP = FastAPI(title='APSVIZ UI Data', version=APP_VERSION)

# declare app access details
APP.add_middleware(CORSMiddleware, allow_origins=['*'], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])


@APP.get('/get_ui_data', status_code=200, response_model=None)
async def get_terria_map_catalog_data(grid_type: Union[str, None] = Query(default=None), event_type: Union[str, None] = Query(default=None),
                                      instance_name: Union[str, None] = Query(default=None), met_class: Union[str, None] = Query(default=None),
                                      storm_name: Union[str, None] = Query(default=None), cycle: Union[str, None] = Query(default=None),
                                      advisory_number: Union[str, None] = Query(default=None), run_date: Union[str, None] = Query(default=None),
                                      end_date: Union[str, None] = Query(default=None), limit: Union[int, None] = Query(default=4)) -> json:
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
    <br/>&nbsp;&nbsp;&nbsp;limit: Limit the number of catalog records returned (default is 4)
    """
    # pylint: disable=unused-argument
    # pylint: disable=too-many-arguments
    # pylint: disable=too-many-locals

    # init the returned html status code
    status_code: int = 200

    try:
        # create the postgres access object
        pg_db: PGUtils = PGUtils(apsviz_dbname, apsviz_username, apsviz_password)

        # init the kwargs variable
        kwargs: dict = {}

        # create the param list
        params: list = ['grid_type', 'event_type', 'instance_name', 'met_class', 'storm_name', 'cycle', 'advisory_number', 'run_date', 'end_date',
                        'limit']

        # loop through the SP params passed in
        for param in params:
            # add this parm to the list
            kwargs.update({param: 'null' if not locals()[param] else f"'{locals()[param]}'"})

        # try to make the call for records
        ret_val: dict = pg_db.get_terria_map_catalog_data(**kwargs)
    except Exception:
        # return a failure message
        ret_val: str = 'Exception detected trying to get the terria map catalog data.'

        # log the exception
        logger.exception(ret_val)

        # set the status to a server error
        status_code = 500

    # return to the caller
    return JSONResponse(content=ret_val, status_code=status_code, media_type="application/json")


@APP.get('/get_terria_map_data_file', status_code=200, response_model=None)
async def get_terria_map_catalog_data_file(file_name: Union[str, None] = Query(default='apsviz.json'),
                                           grid_type: Union[str, None] = Query(default=None), event_type: Union[str, None] = Query(default=None),
                                           instance_name: Union[str, None] = Query(default=None), met_class: Union[str, None] = Query(default=None),
                                           storm_name: Union[str, None] = Query(default=None), cycle: Union[str, None] = Query(default=None),
                                           advisory_number: Union[str, None] = Query(default=None), run_date: Union[str, None] = Query(default=None),
                                           end_date: Union[str, None] = Query(default=None), limit: Union[int, None] = Query(default=4)) -> json:
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
                    'limit']

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
        # create the postgres access object
        pg_db: PGUtils = PGUtils(apsviz_dbname, apsviz_username, apsviz_password)

        # try to make the call for records
        ret_val: dict = pg_db.get_terria_map_catalog_data(**kwargs)

        # write out the data to a file
        with open(temp_file_path, 'w', encoding='utf-8') as f_h:
            json.dump(ret_val, f_h)

    except Exception:
        # log the exception
        logger.exception('')

        # set the status to a server error
        status_code = 500

    # return to the caller
    return FileResponse(path=temp_file_path, filename=file_name, media_type='text/json', status_code=status_code)
