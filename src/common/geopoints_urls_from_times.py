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
    If YAML you must specify these terms.

    Possible scenarios: (options such as instance/ensemble can be applied to all)
        1) Input timein/timeout and the config_name YML (nominal name is url_framework.yml).
               This will generate a set of URLs between the two time ranges.
               This can work for a Hurricane, BUT timein/timeout must be ADVISORY values
        2) Input timeout and offset and the config_name YML (nominal name is url_framework.yml).
               This will generate a set of URLs between the two time ranges.
               This can work for a Hurricane, BUT timeout must be ADVISORY values
        3) Input URL and offset only.
               This will scrape the time/advisory from the URL and offset it in 6-hour steps and
               generate a set of URLs between the two time/advisory ranges.
               This can work for hurricanes

        starttime is the selected time to begin the building of the list (YYYY-mm-dd HH:MM:SS)
        stoptime is the selected time to end the building of the list (YYYY-mm-dd HH:MM:SS)

        Parameters:
        url: (str) A single URL from which more URLs may be built
        ndays: (int) Number of look back/ahead days from the stoptime value
        config_name: (str) path/filename to yaml file that contains the INSTANCE mappings
        hurricane_yaml_source=None: (str) This is a special case.
            If you want to build Hurricane URLs from a YAML, then you will need to specify the subdir name directly, e.g. 'al09'.
            This will replace the default value of a name.
        hurricane_yaml_year: (str) is part of the Hurricane special case. No way to dig out the year directory name without the user specifying it
            only needed for YAML based hurricane construction. Eg .../thredds/dodsC/2021/al09/11/ec95d/...
    """
    def __str__(self):
        return self.__class__.__name__

    def __init__(self, _app_name='GenerateURLsFromTimes.TEST', _logger=None, url=None, time_in=None, time_out=None, n_days=None, grid_name=None,
                 instance_name=None, config_name=None, hurricane_yaml_year=None, hurricane_yaml_source=None):
        # get a handle to a logger
        self.logger = _logger

        self.utils = GeoUtilities(_logger=self.logger)

        stop_time = None

        # The Hurricane special terms are only usedY if you are requesting to build from a YAML AND the caller wants Hurricane data
        # If a URL passed in, then gridname and instance can be gotten from it.
        # ensemble values are expected to be changed by the user
        self.config_name = config_name
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

            self.hurricane_yaml_source = hurricane_yaml_source
            self.hurricane_yaml_year = hurricane_yaml_year  # Cannot span multiple years using Hurricane-YAML construction

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
            words = url.split('/')
            words[-2] = ensemble
            words[-3] = self.instance_name
            words[-6] = str(time)  # Need this in case it is an advisory value
            newurl = '/'.join(words)
            if newurl not in urls:
                urls.append(newurl)
        self.logger.debug('Constructed %s urls of ensemble %s', urls, ensemble)
        return urls

    @staticmethod
    def load_config(config_name):
        """
        placeholder method to load the config file

        :param config_name:
        :return:
        """
        return config_name

    # Approach Used by ADDA
    def build_url_list_from_yaml_and_times(self, ensemble='nowcast') -> list:
        """
        We seek to build a set of compatible URLs spanning the input time range based on the
        structure of asgs urls in the config_name. The structure of the output URLs will be based on the 
        entries in the associated YAML file. Since, no url will be provided, we must ask the caller to provide
        the gridname, ensemble, and instance. We expect the caller to provide a proper Instance value
        for the new URLs. 
        We REQUIRE the grid name. Only change in the ensemble and times are expected
   
        This uses the following class variables:
        time_range: (tuple) (datetime, datetime). Time range inclusive (could also be hurricane advisories)
        instance: (str) if set the used for all urls. If not, attempt to find it in the YAML
        gridname: (str) name for the grid

        Parameters:
            ensemble: (str) ensemble name (dafaults to nowcast)

        Returns:
            urls: list(str). List of valid URLs for processing
        """
        config = None

        if self.config_name is None:
            raise Exception('self.config_name is None. Cannot use the YAML generators: Abort')

        try:
            config = self.load_config(self.config_name)
        except FileNotFoundError as e:  # OSError:
            raise FileNotFoundError(f'No URL structural config yml file found: {self.config_name}: Abort') from e

        time_range = (self.start_time, self.stop_time)  # Could also be a range of advisories
        list_of_times = self.utils.generate_six_hour_time_steps_from_range(time_range)
        list_of_instances = self.utils.generate_list_of_instances(list_of_times, self.grid_name, self.instance_name)

        urls = []

        self.logger.debug('list_of_times: %s', list_of_times)
        self.logger.debug('list_of_instances: %s', list_of_instances)

        for time, instance in zip(list_of_times, list_of_instances):
            url = self.utils.construct_url_from_yaml(config, time, self.instance_name, ensemble, self.grid_name,
                                                     hurricane_yaml_year=self.hurricane_yaml_year, hurricane_yaml_source=self.hurricane_yaml_source)
            if url not in urls:
                urls.append(url)

        self.logger.debug('Constructed %s urls of ensemble %s based on the YML', urls, ensemble)
        return urls

    # Approach Used by ADDA
    def build_url_list_from_yaml_and_offset(self, ensemble='nowcast') -> list:
        """
        We seek to build a set of compatible URLs spanning the input time range based on the
        structure of asgs urls in the config_name. The structure of the output URLs will be based on the 
        entries in the associated YAML file. Since, no url will be provided, we must ask the caller to provide
        the gridname, ensemble, and instance. We expect the caller to provide a proper Instance value
        for the new URLs. 
        We REQUIRE the grid name. Only change in the ensemble and times are expected
   
        Uses the following class variables:
        offset: (int). The offset in days
        instance: (str) if set then used for all urls
        gridname: (str) name for the grid
        ensemble: (str) ensemble name (dafaults to nowcast)

        Parameters:
            ensemble: (str) ensemble name (dafaults to nowcast)

        Returns:
            urls: list(str). List of valid URLs for processing
        """
        config = None

        if self.config_name is None:
            raise Exception('self.config_name is None. Cannot use the YAML generators: Abort')

        try:
            config = self.load_config(self.config_name)
        except OSError as e:
            raise OSError(f'No URL structural config yml file {self.config_name} found: Abort') from e

        time_value = self.stop_time  # Could also be an advisory
        offset = self.n_days
        if offset > 0:
            self.logger.warning('Offset >0 specified: Behavior is not tested')

        list_of_times = self.utils.generate_six_hour_time_steps_from_offset(time_value, offset)
        list_of_instances = self.utils.generate_list_of_instances(list_of_times, self.grid_name, self.instance_name)

        urls = []

        for time, instance in zip(list_of_times, list_of_instances):
            url = self.utils.construct_url_from_yaml(config, time, self.instance_name, ensemble, self.grid_name,
                                                     hurricane_yaml_year=self.hurricane_yaml_year, hurricane_yaml_source=self.hurricane_yaml_source)
            if url not in urls:
                urls.append(url)

        self.logger.warning('Constructed %s urls of ensemble %s based on the YML and offset', urls, ensemble)

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
            log_level, log_path = LoggingUtil.prep_for_logging()

            # create a logger
            self.logger = LoggingUtil.init_logging(_app_name, level=log_level, line_format='medium', log_file_path=log_path)

    def run(self, args):
        """
        A simple main method to demonstrate the use of this class
        """

        config_name = args.config_name if args.config_name is not None else os.path.join(os.path.dirname(__file__), '../config', 'url_framework.yml')

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
                                            grid_name=None, instance_name=None, config_name=None)
                new_urls = rpl.build_url_list_from_template_url_and_times(ensemble=args.ensemble)
            else:
                self.logger.debug('Selecting time+ndays procedure')
                rpl = GenerateURLsFromTimes(_logger=self.logger, url=args.url, time_in=None, time_out=args.timeout, n_days=args.ndays, grid_name=None,
                                            instance_name=None, config_name=None)
                new_urls = rpl.build_url_list_from_template_url_and_offset(ensemble=args.ensemble)
        else:
            self.logger.debug('Selecting a YAML generation method')
            if args.grid_name is None or args.instance_name is None or config_name is None:
                raise Exception('YAML-based procedures requires gridname, instance_name and config_name')
            if args.hurricane_yaml_year is not None and args.hurricane_yaml_source is not None:
                self.logger.debug('Detected values required for building YAML-based Hurricane urls')
            if args.timein is not None:
                self.logger.debug('Selecting a specific time-range procedure')
                rpl = GenerateURLsFromTimes(_logger=self.logger, time_in=args.timein, time_out=args.timeout, n_days=None, grid_name=args.grid_name,
                                            instance_name=args.instance_name, config_name=args.config_name,
                                            hurricane_yaml_year=args.hurricane_yaml_year, hurricane_yaml_source=args.hurricane_yaml_source)
                new_urls = rpl.build_url_list_from_yaml_and_times(ensemble=args.ensemble)
            else:
                self.logger.debug('Selecting time+ndays procedure')
                rpl = GenerateURLsFromTimes(_logger=self.logger, time_in=None, time_out=args.timeout, n_days=args.ndays, grid_name=args.grid_name,
                                            instance_name=args.instance_name, config_name=args.config_name,
                                            hurricane_yaml_year=args.hurricane_yaml_year, hurricane_yaml_source=args.hurricane_yaml_source)
                new_urls = rpl.build_url_list_from_yaml_and_times(ensemble=args.ensemble)

        self.logger.debug('New urls: %s', new_urls)


if __name__ == '__main__':
    # setup a logger for testing
    logger = LoggingUtil.init_logging("GenerateURLsFromTimes.test", level=10, line_format='medium',
                                      log_file_path='./geopoints_url_from_times-test.log')

    from argparse import ArgumentParser

    parser = ArgumentParser()
    parser.add_argument('--url', default=None, action='store', dest='url', help='Input URL that may be used to build new output urls', type=str)
    parser.add_argument('--ndays', default=None, action='store', dest='ndays', help='Day lag (usually < 0)', type=int)
    parser.add_argument('--timeout', default=None, action='store', dest='timeout', help='YYYY-mm-dd HH:MM:SS. Latest day of analysis', type=str)
    parser.add_argument('--timein', default=None, action='store', dest='timein', help='YYYY-mm-dd HH:MM:SS .Start day of analysis. ', type=str)
    parser.add_argument('--config_name', action='store', dest='config_name', default=None,
                        help='String: yml config which contains URL structural information')
    parser.add_argument('--instance_name', action='store', dest='instance_name', default=None,
                        help='String: Choose instance name. Required if using a YAML-based URL construction')
    parser.add_argument('--grid_name', action='store', dest='grid_name', default=None,
                        help='String: Choose grid_name. Required if using a YAML-based URL construction')
    parser.add_argument('--ensemble', action='store', dest='ensemble', default='nowcast', help='String: Specify ensemble name ')
    parser.add_argument('--hurricane_yaml_year', action='store', dest='hurricane_yaml_year', default=None,
                        help='String: Needed only for Hurricane/YML procedures')
    parser.add_argument('--hurricane_yaml_source', action='store', dest='hurricane_yaml_source', default=None,
                        help='String: Needed only for Hurricane/YML procedures')

    cli_args = parser.parse_args()

    # log the input args
    logger.debug('input cli_args: %s', cli_args)

    gen_entry = GenerateURLsEntry(_logger=logger)

    gen_entry.run(cli_args)
