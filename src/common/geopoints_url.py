'''
MIT License

Copyright (c) 2022,2023,2024 Renaissance Computing Institute

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
'''

## Codes substantially derived from the utilities.py code written by Brian O. Blanton, RENCI

#!/usr/bin/env python
# coding: utf-8

import sys
import pandas as pd
import numpy as np
import time as tm
import src.common.utilities as utilities
import src.common.generate_urls_from_times as genurls
from argparse import ArgumentParser

# create a logger
logger = utilities.logger

# Define some basic mappings for URL to variables names. Can override using CI variables
var_mapper={'fort':'zeta','swan':'swan_HS'}

def guess_variable_name(url)->str:
    """
    Simply search the given URL for occurances of ither fort or swan. Choose the variable approapriately. User may always
    override using --variable_name

    Parameters:
        url: (str). A valid urls
    Returns:
        varname: <str>. Guess is varname is zeta or swan_HS based on url nomenclature and specifications in the var_mapper dict
    """
    varname=None
    for key,value in var_mapper.items():
        if isinstance(key, str) and key.casefold() in url.casefold():
            varname=value
            break
    return varname

def strip_ensemble_from_url(urls)->str:
    """
    We mandate that the URLs input to this fetcher are those used to access the TDS server used in APSViz. The "ensemble" information will be in position .split('/')[-2]
    eg. 'http://tds.renci.org/thredds/dodsC/2021/nam/2021052318/hsofs/hatteras.renci.org/hsofs-nam-bob-2021/nowcast/fort.63.nc'
    
    Parameters:
        urls: list(str). list of valid urls
    Returns:
        Ensemble: <str>
    """
    url = grab_first_url_from_urllist(urls)
    try:
        words = url.split('/')
        ensemble=words[-2] # Usually nowcast,forecast, etc 
    except IndexError as e:
        logger.exception(f'strip_ensemble_from_url Unexpected failure try next:')
    return ensemble

def first_true(iterable, default=False, pred=None):
    """
    itertools recipe found in the Python 3 docs
    Returns the first true value in the iterable.
    If no true value is found, returns *default*
    If *pred* is not None, returns the first item
    for which pred(item) is true.

    first_true([a,b,c], x) --> a or b or c or x
    first_true([a,b], x, f) --> a if f(a) else b if f(b) else x

    """
    return next(filter(pred, iterable), default)

def grab_first_url_from_urllist(urls)->str:
    """
    eg. 'http://tds.renci.org/thredds/dodsC/2021/nam/2021052318/hsofs/hatteras.renci.org/hsofs-nam-bob-2021/nowcast/fort.63.nc'
    
    Parameters:
        urls: list(str). list of valid urls
    Returns:
        url: <str> . Fetch first available, valid url in the list
    """
    if not isinstance(urls, list):
        logger.error('first url: URLs must be in list form')
        sys.exit(1)
    url = first_true(urls)
    return url

def main(args):
    variable_name=args.variable_name
    url=args.url
    lon=args.lon
    lat=args.lat
    nearest_neighbors=args.kmax
    ndays=args.ndays # Look back/forward 

    if variable_name is None:
        variable_name=guess_variable_name(url)
    if variable_name is None:
        logger.error('Variable name invald or not identified')
        sys.exit(1)
    logger.info(f' Identified variable name is {variable_name}')

    ensemble=strip_ensemble_from_url([url])
    if args.ensemble is not None: # Else use the ensemble present in the input URL. Allow us to input a forecast but choose the nowcast
        ensemble = args.ensemble
    logger.info(f'Input URL ensemble determined to be {ensemble}')

    # Try to setup proper header names for ADC/SWN and for nowcast/forecasr
    dataproduct='Forecast'
    if ensemble=='nowcast':
        dataproduct='Nowcast'
    # Now figure out data source: adcirc or swan
    datasrc='APS'
    if variable_name=='swan_HS':
        datasrc='SWAN'
    headername=f'{datasrc} {dataproduct}'
    logger.info(f' Header name defined to be {headername}')

    if ndays <= 0:
        logger.info(f'Build list of URLs to fetch: ndays lookback is {ndays}')
        rpl = genurls.generate_urls_from_times(url=url,timein=None, timeout=None, ndays=ndays, grid_name=None, instance_name=None, config_name=None)
        new_urls = rpl.build_url_list_from_template_url_and_offset(ensemble=ensemble)
        logger.debug('New URL list %s', new_urls)
    else:
        new_urls=[url]
    logger.info('Number of URL to try and process is: %s', len(new_urls))

    logger.debug('Lon: %s, Lat: %s', lon, lat)
    logger.debug('Selected nearest neighbors values is: %s', nearest_neighbors)

    if len(new_urls) ==0:
        logger.exit('No URLs identified given the input URL: %s. Abort', url)

    data_list=list()
    exclude_list=list()

    t0=tm.time()
    for url in new_urls:
        logger.debug('URL: %s', url)
        try:
            df_product_data, df_product_metadata, df_excluded = utilities.Combined_pipeline(url, variable_name, lon, lat, nearest_neighbors)
            #df_product_data.to_csv(f'Product_data.csv',header=args.keep_headers)
            #df_product_metadata.to_csv(f'Product_meta.csv',header=args.keep_headers)
            data_list.append(df_product_data)
            exclude_list.append(df_excluded)
        except (OSError,FileNotFoundError):
            logger.warning('Current URL was not found: %s. Try another...', url)
            pass
    logger.info('Fetching Runtime was: %s seconds', tm.time()-t0)

    df=pd.concat(data_list,axis=0)
    df.columns=[headername]
    df = (df.reset_index()
        .drop_duplicates(subset='index', keep='last')
        .set_index('index').sort_index())
    df_excluded=pd.concat(exclude_list,axis=0)

    df.index = df.index.strftime('%Y-%m-%d %H:%M:%S')
    df.index.name='time'

    logger.debug('Dimension of final data array: %s', df.shape)
    logger.debug('Dimension of excluded URL list array: %s', df_excluded.shape)

    # Final data outputs
    # df.to_csv('Product_data_geopoints.csv')
    # df_excluded.to_csv('Product_excluded_geopoints.csv')
    
    logger.info('Finished. Runtime was: %s seconds', tm.time()-t0)
    return df

if __name__ == '__main__':
    ret_val=0

    try:
        parser = ArgumentParser()
        parser.add_argument('--lon', action='store', dest='lon', default=None, type=float,
       	                   help='lon: longitiude value for time series extraction')
        parser.add_argument('--lat', action='store', dest='lat', default=None, type=float,
                           help='lat: latitude value for time series extraction')
        parser.add_argument('--variable_name', action='store', dest='variable_name', default=None, type=str,
                           help='Optional variable name of interest from the supplied url')
        parser.add_argument('--kmax', action='store', dest='kmax', default=10, type=int,
                           help='nearest_neighbors values when performing the Query')
        parser.add_argument('--alt_urlsource', action='store', dest='alt_urlsource', default=None, type=str,
                           help='Alternative location for the ADCIRC data - NOTE specific formatting requirements exist')
        parser.add_argument('--url', action='store', dest='url', default=None, type=str,
                           help='Specify FQ URL')
        parser.add_argument('--keep_headers', action='store_true', default=True,
                           help='Boolean: Indicates to add header names to output files')
        parser.add_argument('--ensemble', action='store', dest='ensemble', default=None, type=str,
                           help='Choose overriding ensemble such as nowcast. Else internal code extracts from the URL')
        parser.add_argument('--ndays', action='store', dest='ndays', default=0, type=int,
                           help='ndays to scan: Default=0, <0 means look back. >0 means look forward')
        args = parser.parse_args()

    	# log the input args
        logger.debug('input args: %s',args)

        # Call the runner
        df = main(args)

        logger.debug('Final output df:%s:%s',df.head(),df.shape)

    except Exception:
        logger.exception("Exit: exception occured")
        ret_val=1

    sys.exit(ret_val)

