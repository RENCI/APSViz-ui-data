#!/usr/bin/env python

# SPDX-FileCopyrightText: 2022 Renaissance Computing Institute. All rights reserved.
# SPDX-FileCopyrightText: 2023 Renaissance Computing Institute. All rights reserved.
#
# SPDX-License-Identifier: GPL-3.0-or-later
# SPDX-License-Identifier: LicenseRef-RENCI
# SPDX-License-Identifier: MIT

"""
    Time series extraction

    Authors: Jeffrey L. Tilson
"""

import os
import datetime as dt
from src.common.geopoints_utilities import GeoUtilities
from src.common.logger import LoggingUtil


class GenerateURLsFromTimes:
    """
    Class that attempts to create a list of valid TDS Urls based on the input time conditions and possibly a YAML file
    that contains the URL structure
    This is NOT expected to be highly generalized and is intended for the ADDA/AST pipelines

    We hardwire the concept that hurricane data files timestep every 6 hours.

    It is okay to have one or two "wrong" urls in the list as the Harvester should be pretty resilient.

    If the caller elects to define output URLs based on times/ndays, then a YAML describing the desired structure is required.
    If the caller elects to also supply a URL, then the output URL structure will be gleaned from it.
        regardless of instance

    Lastly, the final product data series may extend before or after the stop/start times.

    As an example, if grabbing a nowcast, the data series may begin 6 hours before the indicated url time.

    Pass in a URL and the instance, gridname, are scraped from it
    Former YAML-based URL structural assignments has been deprecated. Refer original AST codes

    starttime is the selected time to begin the building of the list (YYYY-mm-dd HH:MM:SS)
    stoptime is the selected time to end the building of the list (YYYY-mm-dd HH:MM:SS)

    Parameters:
        url: (str) A single URL from which more URLs may be built
        ndays: (int) Number of look back/ahead days from the stoptime value
    """
    def __str__(self):
        return self.__class__.__name__

    def __init__(self, _app_name='GenerateURLsFromTimes.TEST', _logger=None, url=None, time_in=None, time_out=None, n_days=None, grid_name=None,
                 instance_name=None):
        # get a handle to a logger
        self.logger = _logger

        self.utils = GeoUtilities(_logger=self.logger)

        stop_time = None

        # The Hurricane special terms are only usedY if you are requesting to build from a YAML AND the caller wants Hurricane data
        # If a URL passed in, then gridname and instance can be gotten from it.
        # ensemble values are expected to be changed by the user
        if url is not None:
            words = url.split('/')
            self.ensemble = words[-2]
            self.instance_name = words[-3]
            self.grid_name = words[-5]
            try:
                stop_time = dt.datetime.strptime(words[-6], '%Y%m%d%H').strftime('%Y-%m-%d %H:%M:%S')  # Can be overridden by args.stop_time
            except ValueError:  # Must be a hurricane
                stop_time = words[-6]
            self.url = url
        # If No url, then build URLs from a YAML. This requires the caller to specify gridname, instance, and ensemble
        else:
            self.instance_name = instance_name  # This is for potentially mapping new instances to urls
            self.grid_name = grid_name
            if self.instance_name is None:
                raise Exception('Must specify an instance value if building URLs based on a YAML. None specified: Abort')

            if self.grid_name is None:
                raise Exception('Must specify a grid_name if building URLs based on a YAML. None specified: Abort')

        # timeout MUST be supplied somehow
        if time_out is None and stop_time is None:
            raise Exception('timeout is not set and no URL provided: Abort')

        if time_out is not None:
            stop_time = time_out

        # Find time in
        if time_in is None:
            if n_days is None:
                raise Exception('No timein or ndays specified.')

            start_time = self.utils.construct_start_time_from_offset(stop_time, n_days)  # Will return an advisory value if appropriate
        else:
            start_time = time_in

        self.start_time = start_time
        self.stop_time = stop_time
        self.n_days = n_days

        self.logger.debug('Current time (or advisory) range is %s to %s. Specified ndays is %s', self.start_time, self.stop_time, self.n_days)

        if url is not None:
            self.logger.debug('Current estimated ensemble: %s, instance: %s and gridname: %s', self.ensemble, self.instance_name, self.grid_name)

    def build_url_list_from_template_url_and_times(self, ensemble='nowcast') -> list:
        """
        We seek to build a set of compatible URLs spanning the input time range based on the
        structure of the input URL. We expect the caller to provide a proper ensemble value
        for the new URLs. 
        We expect no changes in the grid name. Only change in the ensemble and times are expected
   
        Parameters:
            ensemble: (str) Caller specified ensemble. This way one could input a namforecast url but request nowcasts, e.g.

        Returns:
            urls: list(str). List of valid URLs for processing
        """
        url = self.url
        time_range = (self.start_time, self.stop_time)  # This could also be an advisory range
        list_of_times = self.utils.generate_six_hour_time_steps_from_range(time_range)
        list_of_instances = self.utils.generate_list_of_instances(list_of_times, self.grid_name, self.instance_name)

        urls = []

        for time, instance in zip(list_of_times, list_of_instances):
            self.logger.debug('time: %s, instance: %s', time, instance)
            words = url.split('/')
            words[-2] = ensemble
            words[-3] = self.instance_name
            words[-6] = str(time)  # Need to ensure because we could have an advisory come in
            new_url = '/'.join(words)

            if new_url not in urls:
                urls.append(new_url)

        self.logger.debug('Constructed %s urls of ensemble %s', urls, ensemble)

        return urls

    def build_url_list_from_template_url_and_offset(self, ensemble='nowcast') -> list:
        """
        We seek to build a set of compatible URLs starting from the URL embedded time 
        and walking back/forward offset days while using the provided ensemble value.
        e.g., you might send in a forecast and want back a list of nowcasts for the same grid
        structure of the input URL. We expect the caller to provide a proper ensemble value
        for the new URLs. 
        We expect no changes in the grid name. Only change in the ensemble and times are expected
   
        Parameters:
            ensemble: (str) (def of "nowcast") The desired ensemble word for the resultant urls

        Returns:
            urls: list(str). List of valid URLs for processing
        """
        url = self.url
        time_value = self.stop_time  # Could also be an advisory
        offset = self.n_days

        if offset > 0:
            self.logger.warning('Offset >0 specified: Behavior is not tested')

        # time_in = url.split('/')[-6] # Maybe need to check for a Hurricane Advisory also
        list_of_times = self.utils.generate_six_hour_time_steps_from_offset(time_value, offset)
        list_of_instances = self.utils.generate_list_of_instances(list_of_times, self.grid_name, self.instance_name)

        urls = []

        for time, instance in zip(list_of_times, list_of_instances):
            self.logger.debug('time: %s, instance: %s', time, instance)
            words = url.split('/')
            words[-2] = ensemble
            words[-3] = self.instance_name
            words[-6] = str(time)  # Need this in case it is an advisory value
            newurl = '/'.join(words)
            if newurl not in urls:
                urls.append(newurl)
        self.logger.debug('Constructed %s urls of ensemble %s', urls, ensemble)
        return urls


class GenerateURLsEntry:
    """
    Class that has an entry point to build urls

    """
    def __str__(self):
        return self.__class__.__name__

    def __init__(self, _app_name='GenerateURLsEntry.TEST', _logger=None):
        """
        inits the class

        :param _logger:
        """
        # if a reference to a logger was passed in use it
        if _logger is not None:
            # get a handle to a logger
            self.logger = _logger
        else:
            # get the log level and directory from the environment.
            __log_level, __log_path = LoggingUtil.prep_for_logging()

            # create a logger
            self.logger = LoggingUtil.init_logging(_app_name, level=__log_level, line_format='medium', log_file_path=__log_path)

    def run(self, args):
        """
        A simple main method to demonstrate the use of this class
        """

        # init the return
        new_urls: list = []

        # Set up IO env
        self.logger.debug("Product Level Working in %s.", os.getcwd())

        if args.instance_name is not None:
            self.logger.debug('Ignoring args.instance_name for the testing sequence')

        #
        # Need to specify precedence in the arguments provided for testing main
        #

        if args.url is not None:
            self.logger.debug('Selecting a template-url generation method')
            if args.timein is not None:
                self.logger.debug('Selecting a specific time-range procedure')
                rpl = GenerateURLsFromTimes(_logger=self.logger, url=args.url, time_in=args.timein, time_out=args.timeout, n_days=None,
                                            grid_name=None, instance_name=None)
                new_urls = rpl.build_url_list_from_template_url_and_times(ensemble=args.ensemble)
            else:
                self.logger.debug('Selecting time+ndays procedure')
                rpl = GenerateURLsFromTimes(_logger=self.logger, url=args.url, time_in=None, time_out=args.timeout, n_days=args.ndays, grid_name=None,
                                            instance_name=None)
                new_urls = rpl.build_url_list_from_template_url_and_offset(ensemble=args.ensemble)
        else:
            self.logger.debug('No URL was specified')

        self.logger.debug('New urls: %s', new_urls)


if __name__ == '__main__':
    # get the log level and directory from the environment.
    log_level, log_path = LoggingUtil.prep_for_logging()

    # setup a logger for testing
    logger = LoggingUtil.init_logging("GenerateURLsFromTimes.test", level=log_level, line_format='medium', log_file_path=log_path)

    from argparse import ArgumentParser

    parser = ArgumentParser()
    parser.add_argument('--url', required=True, action='store', dest='url', help='Input URL that may be used to build new output urls', type=str)
    parser.add_argument('--ndays', default=None, action='store', dest='ndays', help='Day lag (usually < 0)', type=int)
    parser.add_argument('--timeout', default=None, action='store', dest='timeout', help='YYYY-mm-dd HH:MM:SS. Latest day of analysis', type=str)
    parser.add_argument('--timein', default=None, action='store', dest='timein', help='YYYY-mm-dd HH:MM:SS .Start day of analysis. ', type=str)
    parser.add_argument('--instance_name', action='store', dest='instance_name', default=None,
                        help='String: Choose instance name. Required if using a YAML-based URL construction')
    parser.add_argument('--grid_name', action='store', dest='grid_name', default=None,
                        help='String: Choose grid_name. Required if using a YAML-based URL construction')
    parser.add_argument('--ensemble', action='store', dest='ensemble', default='nowcast', help='String: Specify ensemble name ')

    cli_args = parser.parse_args()

    # log the input args
    logger.debug('input cli_args: %s', cli_args)

    gen_entry = GenerateURLsEntry(_logger=logger)

    gen_entry.run(cli_args)
