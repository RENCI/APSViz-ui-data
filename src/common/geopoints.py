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

            # create a named tuple for the args to mimic the cli input
            argsNT: namedtuple = namedtuple('argsNT',
                                            ['lon', 'lat', 'variable_name', 'kmax', 'alt_urlsource', 'url', 'keep_headers',
                                             'ensemble', 'ndays'])

            # test setup params
            # k port-forward <timeseries DB> 5437:5432 &
            # k port-forward <apsviz DB> 5434:5432 &

            # lat: 32.8596518752
            # lon: -79.6725155674
            # variable_name: None
            # kmax: 10
            # alt_urlsource: None
            # url: <TDS_SVR>/thredds/dodsC/2023/al4/16/NCSC_SAB_v1.23/ht-ncfs.renci.org/ncsc123_nhc_al042023/ofcl/fort.63.nc
            # keep_headers: True
            # ensemble: nowcast
            # ndays: 0

            print(kwargs)

            # init the named tuple for the nowcast call
            args = argsNT(float(kwargs['lon']), float(kwargs['lat']), kwargs['variable_name'], int(kwargs['kmax']), kwargs['alt_urlsource'],
                          kwargs['url'], bool(kwargs['keep_headers']), kwargs['ensemble'], int(kwargs['ndays']))

            # call the function, check the return
            df_nc = gu.main(args)

            # init the named tuple for the forecast call
            # note that the ensemble is defaulted for forecasts
            args = argsNT(float(kwargs['lon']), float(kwargs['lat']), kwargs['variable_name'], int(kwargs['kmax']), kwargs['alt_urlsource'],
                          kwargs['url'], bool(kwargs['keep_headers']), None, int(kwargs['ndays']))

            # call the function, check the return
            df_fc = gu.main(args)

            # join the results
            df_join = df_nc.join(df_fc, how='outer')

            # assign the return.
            ret_val = df_join.to_csv()

        except Exception:
            self.logger.exception('Exception getting the geo-point data.')

        self.logger.debug('End')

        # return the data
        return ret_val
