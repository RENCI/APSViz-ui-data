"""
MIT License

Copyright (c) 2022,2023,2024 Renaissance Computing Institute

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to
deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
IN THE SOFTWARE.

    Time series extraction

    Authors: Jeffrey L. Tilson, Brian O. Blanton 8/2024
"""

import sys
import time as tm
import pandas as pd

from src.common.geopoints_urls_from_times import GenerateURLsFromTimes
from src.common.geopoints_utilities import GeoUtilities
from src.common.logger import LoggingUtil


class GeoPointsURL:
    """
    Class for geo-point functionality.

    """
    def __init__(self, app_name='GeoPointsURL.TEST', _logger=None):
        """
        Entry point for the GeoPointsURL class
        """
        # if a reference to a logger was passed in use it
        if _logger is not None:
            # get a handle to a logger
            self.logger = _logger
        else:
            # get the log level and directory from the environment.
            log_level, log_path = LoggingUtil.prep_for_logging()

            # create a logger
            self.logger = LoggingUtil.init_logging(app_name, level=log_level, line_format='medium', log_file_path=log_path)

        # Define some basic mappings for URL to variables names. Can override using CI variables
        self.var_mapper = {'fort': 'zeta', 'swan': 'swan_HS'}

        # create the utility class
        self.geo_utils = GeoUtilities(_logger=self.logger)

    def guess_variable_name(self, url) -> str:
        """
        Search the given URL for occurrences of either fort or swan and choose the variable appropriately.

        User may always override using --variable_name

        Parameters:
            url: (str) valid urls
        Returns:
            varname: <str> Guess is varname is zeta or swan_HS based on url nomenclature and specifications in the var_mapper dict
        """
        varname = None

        for key, value in self.var_mapper.items():
            if isinstance(key, str) and key.casefold() in url.casefold():
                varname = value
                break

        return varname

    def strip_ensemble_from_url(self, urls) -> str:
        """
        We mandate that the URLs input to this fetcher are those used to access the TDS server used in APSViz.
        The "ensemble" information will be in position .split('/')[-2]

        e.g., 'https://tds.renci.org/thredds/dodsC/2021/nam/2021052318/hsofs/hatteras.renci.org/hsofs-nam-bob-2021/nowcast/fort.63.nc'

        Parameters:
            urls: list(str) list of valid urls
        Returns:
            Ensemble: <str>
        """
        url = self.grab_first_url_from_url_list(urls)
        ensemble = None

        try:
            words = url.split('/')

            ensemble = words[-2]  # Usually nowcast, forecast, etc.
        except IndexError:
            self.logger.exception('strip_ensemble_from_url Unexpected failure try next')

        return ensemble

    @staticmethod
    def first_true(iterable, default=False, pred=None):
        """
        itertools recipe found in the Python 3 docs
        Returns the first true value in the iterable.
        If no true value is found, returns *default*
        If *pred* is not None, returns the first item
        for which pred(item) is true.

        first_true([a, b, c], x) --> a or b or c or x
        first_true([a, b], x, f) --> a if f(a) else b if f(b) else x

        """
        return next(filter(pred, iterable), default)

    def grab_first_url_from_url_list(self, urls) -> str:
        """
        e.g., https://tds.renci.org/thredds/dodsC/2021/nam/2021052318/hsofs/hatteras.renci.org/hsofs-nam-bob-2021/nowcast/fort.63.nc

        Parameters:
            urls: list(str). list of valid urls
        Returns:
            url: <str>. Fetch first available, valid url in the list
        """
        # init the return
        url = None

        if not isinstance(urls, list):
            self.logger.error('first url: URLs must be in list form')
        else:
            url = self.first_true(urls)

        return url

    def run(self, args):
        """
        initiates the process

        :param args:
        :return:
        """
        # assign the incoming run arguments
        variable_name = args.variable_name
        url = args.url
        lon = args.lon
        lat = args.lat
        nearest_neighbors = args.kmax
        n_days = args.ndays  # Look back/forward

        self.logger.info('Input URL word is %s', url)

        if variable_name is None:
            variable_name = self.guess_variable_name(url)

        if variable_name is None:
            raise Exception('Variable name invalid or not identified')

        self.logger.debug('Identified variable name is %s', variable_name)

        ensemble = self.strip_ensemble_from_url([url])

        if args.ensemble is not None:  # Else use the ensemble present in the input URL. Allow us to input a forecast but choose the nowcast
            ensemble = args.ensemble

        self.logger.debug('Input URL ensemble determined to be %s', ensemble)

        # Try to set up proper header names for ADC/SWN and for nowcast/forecast
        dataproduct = 'Forecast'

        if ensemble == 'nowcast':
            dataproduct = 'Nowcast'

        # Now figure out the data source: adcirc or swan
        data_src = 'APS'

        if variable_name == 'swan_HS':
            data_src = 'SWAN'

        header_name = data_src + ' ' + dataproduct

        self.logger.debug('Header name defined to be %s ', header_name)

        if n_days <= 0:
            self.logger.debug('Build list of URLs to fetch: n_days lookback is %s', n_days)

            rpl = GenerateURLsFromTimes(_logger=self.logger, url=url, time_in=None, time_out=None, n_days=n_days, grid_name=None, instance_name=None,
                                        config_name=None)

            new_urls = rpl.build_url_list_from_template_url_and_offset(ensemble=ensemble)

            self.logger.info('New URL list %s', new_urls)
        else:
            new_urls = [url]

        self.logger.debug('Number of URL to try and process is: %s', len(new_urls))

        self.logger.info('Lon: %s, Lat: %s, Selected nearest neighbors values is: %s', lon, lat, nearest_neighbors)

        if len(new_urls) == 0:
            raise Exception('No URLs identified given the input URL: %s. Abort')

        data_list = []
        exclude_list = []

        t0 = tm.time()

        for url in new_urls:
            self.logger.debug('URL: %s', url)

            try:
                df_product_data, df_excluded = self.geo_utils.combined_pipeline(url, variable_name, lon, lat, nearest_neighbors)
                # , df_product_metadata
                # df_product_data.to_csv(f'Product_data.csv', header=args.keep_headers)
                # df_product_metadata.to_csv(f'Product_meta.csv', header=args.keep_headers)
                data_list.append(df_product_data)
                exclude_list.append(df_excluded)
            except (OSError, FileNotFoundError):
                self.logger.warning('Current URL was not found: %s. Try another...', url)

        self.logger.info('Fetching Runtime was: %s seconds', tm.time() - t0)

        # init the return
        df = None

        # If absolutely nothing comes back return a None
        try:
            df = pd.concat(data_list, axis=0)
            df.columns = [header_name]
            df = (df.reset_index().drop_duplicates(subset='index', keep='last').set_index('index').sort_index())
            df_excluded = pd.concat(exclude_list, axis=0)
            df.index = df.index.strftime('%Y-%m-%d %H:%M:%S')
            df.index.name = 'time'

            self.logger.debug('Dimension of final data array: %s', df.shape)
            self.logger.debug('Dimension of excluded URL list array: %s', df_excluded.shape)
        except ValueError:
            self.logger.info('No data found for the specified lon/lat air. Return None')

        # Final data outputs
        # df.to_csv('Product_data_geopoints.csv')
        # df_excluded.to_csv('Product_excluded_geopoints.csv')

        self.logger.info('Finished. Runtime was: %s seconds', tm.time() - t0)
        return df


if __name__ == '__main__':
    # Main entry point for local testing

    # init the return
    RET_VAL = 0

    # setup a logger for testing
    logger = LoggingUtil.init_logging("GeoPointsURL.test", level=10, line_format='medium', log_file_path='./geopoints_url-test.log')

    try:
        from argparse import ArgumentParser

        parser = ArgumentParser()

        parser.add_argument('--lon', action='store', dest='lon', default=None, type=float, help='lon: longitude value for time series extraction')
        parser.add_argument('--lat', action='store', dest='lat', default=None, type=float, help='lat: latitude value for time series extraction')
        parser.add_argument('--variable_name', action='store', dest='variable_name', default=None, type=str,
                            help='Optional variable name of interest from the supplied url')
        parser.add_argument('--kmax', action='store', dest='kmax', default=10, type=int, help='nearest_neighbors values when performing the Query')
        parser.add_argument('--alt_urlsource', action='store', dest='alt_urlsource', default=None, type=str,
                            help='Alternative location for the ADCIRC data - NOTE specific formatting requirements exist')
        parser.add_argument('--url', action='store', dest='url', default=None, type=str, help='Specify FQ URL')
        parser.add_argument('--keep_headers', action='store_true', default=True, help='Boolean: Indicates to add header names to output files')
        parser.add_argument('--ensemble', action='store', dest='ensemble', default=None, type=str,
                            help='Choose overriding ensemble such as nowcast. Else internal code extracts from the URL')
        parser.add_argument('--ndays', action='store', dest='ndays', default=0, type=int,
                            help='ndays to scan: Default=0, <0 means look back. >0 means look forward')

        cli_args = parser.parse_args()

        # log the input args
        logger.debug('Input args: %s', cli_args)

        # instantiate the geo-point URL class
        gp_url = GeoPointsURL(logger)

        # Call the runner
        df_out = gp_url.run(cli_args)

        if df_out is not None:
            logger.debug('Final output df: %s:%s', df_out.head(5), df_out.shape)
        else:
            logger.debug('Final output df is None: No data found')

    except Exception:
        logger.exception("Exit: exception occurred")
        RET_VAL = 1

    sys.exit(RET_VAL)
