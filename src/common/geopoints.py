# SPDX-FileCopyrightText: 2022 Renaissance Computing Institute. All rights reserved.
# SPDX-FileCopyrightText: 2023 Renaissance Computing Institute. All rights reserved.
# SPDX-FileCopyrightText: 2024 Renaissance Computing Institute. All rights reserved.
#
# SPDX-License-Identifier: GPL-3.0-or-later
# SPDX-License-Identifier: LicenseRef-RENCI
# SPDX-License-Identifier: MIT

"""
    GeoPoint utilities.

    Author: Phil Owen, 10/11/2024
"""
from collections import namedtuple
import pandas as pd

from src.common.logger import LoggingUtil
import src.common.geopoints_url as gu


class GeoPoint:
    """
        Class that gathers CSV geo point information.
    """

    def __init__(self, _logger=None):
        """
        inits the class

        :return:
        """

        # if this has a reference to a logger passed in use it
        if _logger is not None:
            # get a handle to a logger
            self.logger = _logger
        else:
            # get the log level and directory from the environment.
            log_level, log_path = LoggingUtil.prep_for_logging()

            # create a logger
            self.logger = LoggingUtil.init_logging("APSViz.UI-data.GeoPoint", level=log_level, line_format='medium', log_file_path=log_path)

    def get_geo_point_data(self, **kwargs) -> str:
        """
        gets the geo point data

        :param **kwargs

        :return:
        """
        # init the returned value
        ret_val = ''

        try:
            self.logger.debug('Start')

            # init the return
            ret_val: str = ''

            # build the url to the TDS data
            tds_svr = 'http://' + kwargs['tds_svr'] + '/thredds' + kwargs['url']

            # create a named tuple for the args to mimic the cli input
            argsNT: namedtuple = namedtuple('argsNT',
                                            ['lon', 'lat', 'variable_name', 'kmax', 'alt_urlsource', 'url', 'keep_headers', 'ensemble', 'ndays'])

            # init the named tuple for the nowcast call
            args = argsNT(float(kwargs['lon']), float(kwargs['lat']), kwargs['variable_name'], int(kwargs['kmax']), kwargs['alt_urlsource'], tds_svr,
                          bool(kwargs['keep_headers']), kwargs['ensemble'], int(kwargs['ndays']))

            # call the function, check the return
            df_nc = gu.main(args)

            # if there was a valid response
            if df_nc is not None:
                # convert the index colum to be a datetime
                df_nc.index = pd.to_datetime(df_nc.index)

                # init the named tuple for the forecast call
                # note that the ensemble is defaulted for forecasts
                args = argsNT(float(kwargs['lon']), float(kwargs['lat']), kwargs['variable_name'], int(kwargs['kmax']), kwargs['alt_urlsource'],
                              tds_svr, bool(kwargs['keep_headers']), None, int(kwargs['ndays']))

                # call the function, check the return
                df_fc = gu.main(args)

                # if there was a valid response
                if df_fc is not None:
                    # convert the index colum to be a datetime
                    df_fc.index = pd.to_datetime(df_fc.index)

                    # join the results
                    df_join = df_nc.join(df_fc, how='outer')

                    # assign the return.
                    ret_val = df_join.to_csv()
                else:
                    raise RuntimeError('Error retrieving the forecast data.')
            else:
                raise RuntimeError('Error retrieving the nowcast data.')
        except Exception as e:
            self.logger.exception('Exception getting the geo-point data.')
            raise e
        finally:
            self.logger.debug('End')

        # return the data
        return ret_val
