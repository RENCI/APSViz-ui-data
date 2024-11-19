"""
    Test geopoints_url - Tests geopoints url functionality

    Authors: Jeffrey L. Tilson, Phil Owen @RENCI.org
"""

import os
from collections import namedtuple
from src.common.geopoints_url import GeoPointsURL


def test_geopoints_url():
    """
        This tests the "geopoints url" functionality

    """
    # get the URL of the TDS server
    tds_svr: str = os.getenv('TDS_SVR', 'https://apsviz-thredds-dev.apps.renci.org/')

    # abort if no TDS server URL env param is declared
    assert tds_svr is not None

    # open the file of test URLs
    try:
        # get the test data for now
        with open(os.path.join(os.path.dirname(__file__), 'url-list.txt'), mode='r', encoding="utf-8") as fh:
            # read in the whole file, split it into lines (URLs)
            urls = fh.read().splitlines()
    except FileNotFoundError:
        assert not 'File not found.'

    # create a named tuple for the args to mimic the cli input
    argsNT: namedtuple = namedtuple('argsNT', ['lon', 'lat', 'variable_name', 'kmax', 'alt_urlsource', 'url',
                                               'keep_headers', 'ensemble', 'ndays'])

    # for each test url
    for url in urls:
        # if the line is commented out
        if url.startswith('#'):
            # skip it
            continue

        # if the URL starts with <TDS_SVR> replace it with the tds_svr env parameter gathered above
        url = url.replace('<TDS_SVR>', tds_svr)

        # init the named tuple for the nowcast call
        args = argsNT(-79.6725155674, 32.8596518752, None, 10, None, url, True, 'nowcast', 0)

        # instantiate the geo-point URL class
        gp_url = GeoPointsURL()

        # call the function
        df_nc = gp_url.run(args)

        # check the return
        assert df_nc is not None

        # init the named tuple for the forecast call
        args = argsNT(-79.6725155674, 32.8596518752, None, 10, None, url, True, None, 0)

        # instantiate the geo-point URL class
        gp_url = GeoPointsURL()

        # call the function,
        df_fc = gp_url.run(args)

        # check the return
        assert df_fc is not None

        # join the results
        ret_val = df_nc.join(df_fc, how='outer')

        print(ret_val)

        # assert any errors
        assert ret_val is not None
