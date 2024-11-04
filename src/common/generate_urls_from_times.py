#!/usr/bin/env python

# SPDX-FileCopyrightText: 2022 Renaissance Computing Institute. All rights reserved.
# SPDX-FileCopyrightText: 2023 Renaissance Computing Institute. All rights reserved.
#
# SPDX-License-Identifier: GPL-3.0-or-later
# SPDX-License-Identifier: LicenseRef-RENCI
# SPDX-License-Identifier: MIT

#
# This helper set of functions takes times,offsets,urls as possible inputs and returns a list of URLs that may be passed
# to the fetch_adcirc_data methods. If the Caller wants to process a LIST of Urls, just call this repeatedly
# then aggregate and check duplicates 
#

"""
    Time series extraction

    Authors: Jeffrey L. Tilson
"""

import os,sys
import numpy as np
import pandas as pd
import datetime as dt

import src.common.utilities as utilities
from argparse import ArgumentParser

# pylint: skip-file

# create a logger
logger = utilities.logger


def is_hurricane(test_val)->bool:
    """
    Determine of the input test val is a Date, an Int or something else
    Parameters: 
        test_val: For a valid time enter a str with dformat %Y-%m-%d %H:%M:%S or %Y%m%d%H
                  For a valid hurricane enter an int
    """
    is_hurricane=False
    try:
        test = dt.datetime.strptime(test_val,'%Y-%m-%d %H:%M:%S') # If fails then not a datetime
    except (ValueError,TypeError):
        try:
            test = dt.datetime.strptime(test_val,'%Y%m%d%H')
        except:
            try:
                outid = int(test_val)
                is_hurricane=True
            except ValueError:
                logger.exception('test indicates not a hurricane nor a casting. Perhaps a format issue ?.  Got %s: Abort', test_val)
                raise
                #sys.exit(1)
    return is_hurricane

def generate_six_hour_time_steps_from_range(time_range)->list:
    """
    Given the input time tuple, return the inclusive set of times that occur on
    the daily 6 hour mark. So on output we would have 00Z,06Z,12Z,18Z times only 

    Parameters: 
        time_range: Tuple (datetime,datetime) of the start and end times (datetime objects)

    Returns:
        list_of_times: list of (str) times in the format: %Y%m%d%H

    """
    if is_hurricane(time_range[0]):
        logger.debug('Determined input time_range URL is a Hurricane')
        list_of_times = generate_six_hour_time_advisories_from_range(time_range)
    else:
        list_of_times = generate_six_hour_time_castings_from_range(time_range)
    return list_of_times

def generate_six_hour_time_castings_from_range(time_range)->list:
    """
    A non-hurricane
    Advisory. We need to distinguish between the two. Note, we can be promiscuous here 
    with the URLs, since urls that do not exist will get trapped by Harvester
    Parameters: 
        time_range: tuple (datetime,datetime)
    Returns:
        list_of_times: list of times/advisories in the a string format to build new urls
   """
    keep_hours=[0,6,12,18]
    starttime = dt.datetime.strptime(time_range[0],'%Y-%m-%d %H:%M:%S')
    stoptime = dt.datetime.strptime(time_range[1],'%Y-%m-%d %H:%M:%S')
    pdtime=pd.date_range(start=starttime, end=stoptime,freq='h') # Doesnt land on 00,06,12,18
    list_of_times=list()
    for time in pdtime:
        if time.hour in keep_hours:
            list_of_times.append(time.strftime('%Y%m%d%H'))
    # Keep input entry as well ?
    list_of_times.append(stoptime.strftime('%Y%m%d%H'))
    list_of_times.sort()
    return list_of_times

def generate_six_hour_time_advisories_from_range(advisory_range)->list:
    """
    Advisory range has no specific time meaning other than generally being every 6 hours
    So simply accept the range as fact. The INPUT advisory number is NOT retained in the
    generated list

    Save Advisories in a leading zero format: "{:02d}".format(adv)

    Parameters: 
        advisory_range: tuple (int,int)
    Returns:
        list_of_advisories: list of times/advisories in the a string format to build new urls
                            includes the input time_step.advisory in the final list
   """
    # How many 6 hour periods can we identify? We need to choose a startpoint. Use the highest time and look back
    startadv=int(advisory_range[0])
    stopadv=int(advisory_range[1])
    if startadv > stopadv:
        startadv,stopadv = stopadv, startadv
    list_of_advisories=list()
    for inc in range(startadv, stopadv):
        list_of_advisories.append("{:02d}".format(inc))
    list_of_advisories=[i for i in list_of_advisories if int(i) > 0]
    # Should we retain the input value ?
    list_of_advisories.append("{:02d}".format(stopadv))
    # A last ditch sort to to be sure
    list_of_advisories.sort()
    return list_of_advisories

# Generates a proper list-time/advisories depending if its a Hurricane or not
def generate_six_hour_time_steps_from_offset(time_value, offset)->list:
    """  
    For an arbitrary URL, we could have a conventional now/forecast OR a Hurricane
    Advisory. We need to distinguish between the two. Note, we can be promiscuous here 
    with the URLs, since urls that do not exist will get trapped by Harvester
    Parameters: 
        time_val: (datetime) Either the time or advisory value from a asgs url
        offset: (int) Number of DAYS to look back/forward from strvalue
            if strvalue is an Advisory then we look back a number of STEPS 
            corresponding to 6 hour intervals based on offset
    Returns:
        timelist: list of times/advisories in the a string format to build new urls
    """
    if is_hurricane(time_value):
        logger.debug('Determined input URL is a Hurricane')
        list_of_times = generate_six_hour_time_advisories_from_offset(time_value,offset) 
    else:
        list_of_times = generate_six_hour_time_castings_from_offset(time_value,offset)
    return list_of_times

def generate_six_hour_time_castings_from_offset(time_value,offset)->list:
    """
    Start with the strtime and build a list of 6hour steps for up to offset days
    We expect the input time to a stoptime and the offsets to be < 0. But, though
    the overall code has not been tested for it, we simply reorder the times
    as necc and proceed

    Parameters: 
       time_value: (datetime) start time
       offset: (int) Number of DAYS to look back/forward from strvalue

    Returns:
        timelist: list of times in a string format to build new urls
    """
    keep_hours=[0,6,12,18]
    stoptime = dt.datetime.strptime(time_value,'%Y-%m-%d %H:%M:%S')
    starttime = stoptime + dt.timedelta(days=offset)

    if starttime > stoptime:
        logger.warning('Stoptime < starttime. Supplied offset was %s days: Reordering', offset)
        starttime,stoptime = stoptime,starttime
        
    return generate_six_hour_time_steps_from_range( (starttime.strftime('%Y-%m-%d %H:%M:%S'), stoptime.strftime('%Y-%m-%d %H:%M:%S')) )
    
def generate_six_hour_time_advisories_from_offset(strtime,offset)->list:
    """
    Start with the strtime and build a list of 6hour steps for up to offset days
    We expect the input time to bve an Advisory number (int)  We also anticipate offsets to be < 0. 
    since offset >0 would apply to Hurricane advisories not performed (but you could do it)

    Here we assume each index is a 6 hour time step. So we just need to decide how many to look back for.
    Harvester will quietly ignore urls that do not exist 

    Save Advisories in a leading zero format: "{:02d}".format(adv)

    Parameters: 
       strvale: (str) time
       offset: (int) Number of DAYS to look back/forward from strvalue

    Returns:
        timelist: list of advisories in the a string format to build new urls
    """
    list_of_advisories=list()
    stop_advisory = int(strtime)
    num_6hour_look_asides = int(24*offset/6)
    range_values= [0,num_6hour_look_asides]
    range_values.sort() # sorts ascending order
    for inc in range(*range_values):
        list_of_advisories.append("{:02d}".format(stop_advisory+inc))
    list_of_advisories=[i for i in list_of_advisories if int(i) >= 0]
    # Keep the input value ?
    list_of_advisories.append("{:02d}".format(stop_advisory))
    # A last ditch sort to to be sure
    list_of_advisories.sort()
    return list_of_advisories


def grab_years_from_time_list(list_of_times)->list:
    """
    Process the input time list to extract a list of Years (str)
    Note: This could be a list of Advisories as well. If so, 
    simply return the advisory number, though it will probably not be used

    Parameters: 
        list_of_times: List of (str) time in the format %Y%m%d%H
    Returns:
        list of years values

    """
    list_of_years=list()
    for time in list_of_times:
        try:
            value=dt.datetime.strptime(time,'%Y%m%d%H').year
        except TypeError:
            value=time 
        list_of_years.append(value)
    return list_of_years


def generate_list_of_instances(list_of_times, in_gridname, in_instance):
    """
    This function matches every entry in the list_of_times with an associated instance.
    The structure of this code is such that, in the future, we may have scenarios where
    the value of the instance may change for a given year. 

    Currently, though, we will simply build a list of identical instances.
    The value of the selected instance may be passed in by the caller

    Parameters: 
       list_of_times: list (str)(%Y%m%d%H) ordered set of instances from which to build new URLs
       in_gridname: current gridname from a representative INPUT url
       in_gridname: current instance from a representative INPUT url

    Returns:
        instance_list: ordered list of instances to use for building a set of new urls. 
    """
    num_entries = len(list_of_times)
    gridname = in_gridname # Get default values
    instance = in_instance

    instance_list = num_entries*[instance]
    return instance_list 


# Expect this to be part of a looped  list of times from which appending will be applied
def construct_url_from_yaml( config, intime, instance, ensemble, gridname, hurricane_yaml_year=None, hurricane_yaml_source=None ):
    """    
    Given a single time (%Y%m%d%H) or advisory, the gridname, instance, and ensemble values
    use the entries in config to build a proper URL
    If applying to Hurricanes, we need to also applyld_url_list_from_yaml_and_timest the values for hurricane_yaml_year, and
    hurricane_yaml_source
    """
    # hurricane_yaml_source is a special case scenario
    if is_hurricane(intime):
        logger.debug('Request for YAML build of Hurricane URL. subdir is %s', hurricane_yaml_source)
        intime = str(intime)
        subdir = hurricane_yaml_year # This is certainly NOT generalized
        source=hurricane_yaml_source
    else:
        subdir = dt.datetime.strptime(intime,'%Y%m%d%H').year
        source='nam'
    cfg = config['ADCIRC']
    url = cfg["baseurl"] + \
          cfg["dodsCpart"] % (subdir,
          source, intime,
          cfg["AdcircGrid"] % (gridname),
          cfg["Machine"],
          cfg["Instance"] % (instance),
          cfg["Ensemble"] % (ensemble),
          cfg["fortNumber"]
          )
    return url


def construct_starttime_from_offset(stoptime,ndays):
    """
    Construct an appropriate starttime given the stoptime and offset.
    NOTE if this is a Hurricane advisory, we return an appropriate  
    advisory assuming each advisory is 6 hoursa in duration. No
    negative advisories are returned

    Parameters:
        stoptime (str) (%Y-%m-%d %H:%M:%S)
        ndays: (int) number of 24 hours days to look back/forward

    """
    if is_hurricane(stoptime):
        num_6hour_look_asides = int(24*ndays/6)
        stopadv=int(stoptime)
        startadv=stopadv+num_6hour_look_asides # We normally assume offset is negative but that is not enforced
        return startadv
    else:
        tstop = dt.datetime.strptime(stoptime,'%Y-%m-%d %H:%M:%S')
        tstart = tstop + dt.timedelta(days=ndays)
        starttime = tstart.strftime('%Y-%m-%d %H:%M:%S')
        return starttime
    
    raise 'Fell out the bottom of construct_starttime_from_offset: Abort'
    ##sys.exit(1)

class generate_urls_from_times(object):
    """ 
    Class that attempts to create a list of valid TDS Urls based on the input time conditions and possibly a YAML file
    that contains the URL structure
    This is NOT expected to be highly generalized and is intended for the ADDA/AST pipelines

    We hardwire the concept that hurricane data files timestep every 6 hours. It is okay to have one or two 
    "wrong" urls in the list as Harvester should be pretty resilient.  

    If the caller elects to define output URLs based on times/ndays, then a YAML decribing the desired structure is required.
    If the caller elects to also supply a URL, then the output URL structure will be gleened from it.
        regardless of instance

    Lastly, the final product data series may extend before or after the the stop/start times. As an example, 
        If grabbing a nowcast, the data series may begine 6 hours before the indicated url time. 

    Pass in a URL and the instance, gridname,  are scraped from it
    If YAML you must specify these terms.

    Possible scenarios: (options such as instance/ensemble can be applied to all)
        1) Input timein/timeout and the config_name YML (nominal name is url_framework.yml). This will 
               generate a set of URLs between the two time ranges. This can work for a Hurricane BUT
               timein/timeout must be ADVISORY values
        2) Input timeout and offset and the config_name YML (nominal name is url_framework.yml). This will 
               generate a set of URLs between the two time ranges. This can work for a Hurricane BUT
               timeout must be ADVISORY values
        3) Input URL and offset only. This will scrape the time/advisory from the URL and offset it in 6 hour steps
               generate a set of URLs between the two time/advisory ranges. This can work for a Hurricanes 

    Parameters: 
        url: (str) A single URL from which more URLs may be built
        ndays: (int) Number of look back/ahead days from the stoptime value 
        starttime: (str) Selected time to begin the building of the list (YYYY-mm-dd HH:MM:SS)
        stoptime: (str) Selected time to end the building of the list (YYYY-mm-dd HH:MM:SS)
        config_name: (str) path/filename to yaml file that contains the INSTANCE mappings
        hurricane_yaml_source=None: (str) This is a special case. If you want to build Hurricane URLs from a YAML, 
            then you will need to specify the subdir name directly, eg 'al09'. This will replace the default value of nam. 
        hurricane_yaml_year: (str) is part of the Hurricane special case. No way to dig out the year directory name without the user specifying it
            only needed for YAML based hurricane construction. Eg .../thredds/dodsC/2021/al09/11/ec95d/...
    """
    
    def __init__(self, url=None, timein=None, timeout=None, ndays=None, grid_name = None, instance_name=None, config_name=None, hurricane_yaml_year=None, hurricane_yaml_source=None):
        # The Hurricane special terms are only usedY if you are requesting to build from a YAML AND the caller wants Hurricane data
        # If a URL passed in, then gridname and instance can be gotten from it. ensembles values are expected to be changed by the user

        self.config_name = config_name
        if url is not None:
            words=url.split('/')
            self.ensemble=words[-2]
            self.instance_name=words[-3]
            self.grid_name=words[-5]
            try:
                stoptime=dt.datetime.strptime(words[-6],'%Y%m%d%H').strftime('%Y-%m-%d %H:%M:%S') # Can be overridden by args.stoptime
            except ValueError: # Must be a hurricane
                stoptime=words[-6]
            self.url=url
        # If No url, then build URLs from a YAML. This requires the caller to specify gridname, instance, and ensemble
        else:
            self.instance_name = instance_name # This is for potentially mapping new instances to urls
            self.grid_name = grid_name
            if self.instance_name is None:
                logger.error('Must specify an instance value if building URLs based on a YAML. None specified: Abort')
                raise
                ##sys.exit(1)
            if self.grid_name is None:
                logger.error('Must specify a grid_name if building URLs based on a YAML. None specified: Abort')
                raise
                ##sys.exit(1)
            self.hurricane_yaml_source=hurricane_yaml_source
            self.hurricane_yaml_year=hurricane_yaml_year # Cannot span multiple years using Hurricane-YAML construction

        # timeout MUST be supplied somehow
        if timeout is None and stoptime is None:
            logger.error('timeout is not set and no URL provided: Abort')
            raise
            ##sys.exit(1)
        if timeout is not None:
            stoptime=timeout

        # Find timein
        if timein is None:
            if ndays is None:
                logger.error('No timein or ndays specified.')
                raise
                ##sys.exit(1)
            else:
                starttime = construct_starttime_from_offset(stoptime,ndays) # Will return an advisory value if appropriate
        else:
            starttime = timein
        self.starttime=starttime
        self.stoptime=stoptime
        self.ndays=ndays
        logger.debug('Current time (or advisory) range is %s to %s. Specified ndays is %s', self.starttime, self.stoptime, self.ndays)
        if url is not None:
            logger.debug('Current estimated ensemble: %s, instance: %s and gridname: %s', self.ensemble, self.instance_name, self.grid_name)

    def build_url_list_from_template_url_and_times(self, ensemble='nowcast')-> list:
        """
        We seek to build a set of compatible URLs spanning the input time range based on the
        structure of the input URL. We expect the caller to provide a proper ensemble value
        for the new URLs. 
        We expect no changes in the grid name. Only change in the ensemble and times are expected
   
        Parameters:
            ensemble: (str) Caller specified ensemble. This way one could input a namforecast url but request nowcasts, eg.

        Returns:
            urls: list(str). List of valid URLs for processing
        """
        url = self.url
        time_range=(self.starttime,self.stoptime) # This could also be an advisory range
        list_of_times = generate_six_hour_time_steps_from_range(time_range)
        list_of_instances = generate_list_of_instances(list_of_times, self.grid_name, self.instance_name)
        urls = list()
        for time,instance in zip(list_of_times,list_of_instances):
            words=url.split('/')
            words[-2]=ensemble
            words[-3]=self.instance_name
            words[-6]=str(time) # Need to ensure because we could have an advisory come in 
            newurl='/'.join(words)
            if newurl not in urls:
                 urls.append(newurl)
        logger.debug('Constructed %s urls of ensemble %s', urls, ensemble)
        return urls

    def build_url_list_from_template_url_and_offset(self, ensemble='nowcast')->list:
        """
        We seek to build a set of compatible URLs starting from the URL embedded time 
        and walking back/forward offset days while using the provided ensemble value.
        Eg, you might send in a forecast and want back a list of nowcasts for the same grid
        structure of the input URL. We expect the caller to provide a proper ensemble value
        for the new URLs. 
        We expect no changes in the grid name. Only change in the ensemble and times are expected
   
        Parameters:
            ensemble: (str)( def of "nowcast") The desired ensemble word for the resultant urls

        Returns:
            urls: list(str). List of valid URLs for processing
        """
        url = self.url
        time_value=self.stoptime  # Could also be an advisory
        offset = self.ndays
        if offset > 0:
            logger.warning('Offset >0 specified: Behavior is not tested')
        #timein = url.split('/')[-6] # Maybe need to check for a Hurricane Advisory also 
        list_of_times = generate_six_hour_time_steps_from_offset(time_value,offset)
        list_of_instances = generate_list_of_instances(list_of_times, self.grid_name, self.instance_name)
        urls = list()
        for time,instance in zip(list_of_times,list_of_instances):
            words=url.split('/')
            words[-2]=ensemble
            words[-3]=self.instance_name
            words[-6]=str(time) # Need this in case its an advisory value
            newurl='/'.join(words)
            if newurl not in urls:
                 urls.append(newurl)
        logger.debug('Constructed %s urls of ensemble %s', urls, ensemble)
        return urls

# Approach Used by ADDA
    def build_url_list_from_yaml_and_times(self, ensemble='nowcast')->list:
        """
        We seek to build a set of compatible URLs spanning the input time range based on the
        structure of asgs urls in the config_name. The structure of the output URLs will be based on the 
        entries in the associated yaml file. Since, no url will be provided, we must ask the caller to provide
        the gridname, ensemble, and instance. We expect the caller to provide a proper Instance value
        for the new URLs. 
        We REQUIRE the grid name. Only change in the ensemble and times are expected
   
        Uses the following class variables:
        time_range: (tuple) (datetime,datetime). Time range inclusive (could also be hurricane advisories)
        instance: (str) if set the used for all urls. If not, attempt to find it in the yaml
        gridname: (str) name for the grid

        Parameters:
            ensemble: (str) ensemble name (dafaults to nowcast)

        Returns:
            urls: list(str). List of valid URLs for processing
        """
        if self.config_name is None:
            logger.error('self.config_name is None. Cannot use the YAML generators: Abort')
            raise
            ##sys.exit(1)
        try:
            config = utilities.load_config(self.config_name)
        except FileNotFoundError: # OSError:
            logger.exception('No URL structural config yml file found: %s: Abort', self.config_name)
            raise
            ##sys.exit(1)
        
        time_range=(self.starttime,self.stoptime) # Could also be a range of advisories
        list_of_times = generate_six_hour_time_steps_from_range(time_range)
        list_of_instances = generate_list_of_instances(list_of_times, self.grid_name, self.instance_name)
        urls = list()
        logger.debug('list_of_times: %s', list_of_times)
        logger.debug('list_of_instances: %s', list_of_instances)
        for time,instance in zip(list_of_times,list_of_instances):
            url = construct_url_from_yaml( config, time, self.instance_name, ensemble, self.grid_name, hurricane_yaml_year=self.hurricane_yaml_year, hurricane_yaml_source=self.hurricane_yaml_source )
            if url not in urls:
                 urls.append(url)
        logger.debug('Constructed %s urls of ensemble %s based on the YML', urls, ensemble)
        return urls

# Approach Used by ADDA
    def build_url_list_from_yaml_and_offset(self, ensemble='nowcast')->list:
        """
        We seek to build a set of compatible URLs spanning the input time range based on the
        structure of asgs urls in the config_name. The structure of the output URLs will be based on the 
        entries in the associated yaml file. Since, no url will be provided, we must ask the caller to provide
        the gridname, ensemble, and instance. We expect the caller to provide a proper Instance value
        for the new URLs. 
        We REQUIRE the grid name. Only change in the ensemble and times are expected
   
        Uses the following class variables:
        offset: (int). Offset in days
        instance: (str) if set then used for all urls
        gridname: (str) name for the grid
        ensemble: (str) ensemble name (dafaults to nowcast)

        Parameters:
            ensemble: (str) ensemble name (dafaults to nowcast)

        Returns:
            urls: list(str). List of valid URLs for processing
        """
        if self.config_name is None:
            raise 'self.config_name is None. Cannot use the YAML generators: Abort'
            ##sys.exit(1)
        try:
            config = utilities.load_config(self.config_name)
        except OSError:
            logger.exception('No URL structural config  yml file found. %s: Abort', self.config_name)
            raise 
            ##sys.exit(1)

        time_value=self.stoptime # Could also be an advisory
        offset = self.ndays
        if offset > 0:
            logger.warning('Offset >0 specified: Behavior is not tested')
                 
        list_of_times = generate_six_hour_time_steps_from_offset(time_value,offset)
        list_of_instances = generate_list_of_instances(list_of_times, self.grid_name, self.instance_name)
        urls = list()
        for time,instance in zip(list_of_times,list_of_instances):
            url = construct_url_from_yaml( config, time, self.instance_name, ensemble, self.grid_name, hurricane_yaml_year=self.hurricane_yaml_year, hurricane_yaml_source=self.hurricane_yaml_source )
            if url not in urls:
                 urls.append(url)
        logger.warning('Constructed %s urls of ensemble %s based on the YML and offset', urls, ensemble)
        return urls

def main(args):
    """
    A simple main method to demonstrate the use of this class
    """

    config_name=args.config_name if args.config_name is not None else os.path.join(os.path.dirname(__file__), '../config', 'url_framework.yml')

    # Set up IO env
    logger.debug("Product Level Working in %s.", os.getcwd())

    if args.instance_name is not None:
        logger.debug('Ignoring args.instance_name for the testing sequence')

    #
    # Need to specify precedence in the arguments provided for testing main
    #

    if  args.url is not None:
        logger.debug('Selecting a template-url generation method')
        if args.timein is not None:
            logger.debug('Selecting a specific time-range procedure')
            rpl = generate_urls_from_times(url=args.url,timein=args.timein, timeout=args.timeout, ndays=None, grid_name=None, instance_name=None, config_name=None)
            new_urls = rpl.build_url_list_from_template_url_and_times(ensemble=args.ensemble)
        else:
            logger.debug('Selecting time+ndays procedure')
            rpl = generate_urls_from_times(url=args.url,timein=None, timeout=args.timeout, ndays=args.ndays, grid_name=None, instance_name=None, config_name=None)
            new_urls = rpl.build_url_list_from_template_url_and_offset(ensemble=args.ensemble)
    else:
        logger.debug('Selecting a YAML generation method') 
        if args.grid_name is None or args.instance_name is None or config_name is None:
            raise 'YAML-based procedurs requires gridname, instance_name and config_name'
            ##sys.exit(1)
        if args.hurricane_yaml_year is not None and args.hurricane_yaml_source is not None:
            logger.debug('Detected values required for building YAML-based Hurricane urls')
        if args.timein is not None:
            logger.debug('Selecting a specific time-range procedure')
            rpl = generate_urls_from_times(timein=args.timein, timeout=args.timeout, ndays=None, grid_name=args.grid_name, 
                instance_name=args.instance_name, config_name=args.config_name, hurricane_yaml_year=args.hurricane_yaml_year,hurricane_yaml_source=args.hurricane_yaml_source)
            new_urls = rpl.build_url_list_from_yaml_and_times(ensemble=args.ensemble)
        else:
            logger.debug('Selecting time+ndays procedure')
            rpl = generate_urls_from_times(timein=None, timeout=args.timeout, ndays=args.ndays, grid_name=args.grid_name,
                instance_name=args.instance_name, config_name=args.config_name, hurricane_yaml_year=args.hurricane_yaml_year,hurricane_yaml_source=args.hurricane_yaml_source)
            new_urls = rpl.build_url_list_from_yaml_and_times(ensemble=args.ensemble)

    logger.debug('New urls: %s', new_urls)


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('--url', default=None, action='store', dest='url', help='Input URL that may be used to build new output urls', type=str)
    parser.add_argument('--ndays', default=None, action='store', dest='ndays',help='Day lag (usually < 0)', type=int)
    parser.add_argument('--timeout', default=None, action='store', dest='timeout', help='YYYY-mm-dd HH:MM:SS. Latest day of analysis', type=str)
    parser.add_argument('--timein', default=None, action='store', dest='timein', help='YYYY-mm-dd HH:MM:SS .Start day of analysis. ', type=str)
    parser.add_argument('--config_name', action='store', dest='config_name', default=None,
                        help='String: yml config which contains URL structural information')
    parser.add_argument('--instance_name', action='store', dest='instance_name', default=None,
                        help='String: Choose instance name. Required if using a YAML-based URL construction')
    parser.add_argument('--grid_name', action='store', dest='grid_name', default=None,
                        help='String: Choose grid_name. Required if using a YAML-based URL construction')
    parser.add_argument('--ensemble', action='store', dest='ensemble', default='nowcast',
                        help='String: Specify ensemble name ')
    parser.add_argument('--hurricane_yaml_year', action='store', dest='hurricane_yaml_year', default=None,
                        help='String: Needed only for Hurricane/YML procedures')
    parser.add_argument('--hurricane_yaml_source', action='store', dest='hurricane_yaml_source', default=None,
                        help='String: Needed only for Hurricane/YML procedures')
    args = parser.parse_args()

    # log the input args
    logger.debug('input args: %s', args)

    sys.exit(main(args))

# cat ../config/local_instance.yml
