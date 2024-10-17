# SPDX-FileCopyrightText: 2022 Renaissance Computing Institute. All rights reserved.
# SPDX-FileCopyrightText: 2023 Renaissance Computing Institute. All rights reserved.
# SPDX-FileCopyrightText: 2024 Renaissance Computing Institute. All rights reserved.
#
# SPDX-License-Identifier: GPL-3.0-or-later
# SPDX-License-Identifier: LicenseRef-RENCI
# SPDX-License-Identifier: MIT

"""
    Settings tests.

    Author: Phil Owen, 6/27/2023
"""
import os
import pandas as pd


def test_geo_point_merge():
    """
    tests the merging of geo point data. there is a presumption here that
    the data key (time) will not overlap over nowcasts to forecasts.

    :return:
    """

    # read in the nowcast data. the data frame created is presumed to be the same as the GeoPoint output
    df_nc = pd.read_csv(os.path.join(os.path.dirname(__file__), 'nowcast_data.csv'), index_col=0)

    # dump results of the load
    print(df_nc.head())

    # read in the forecast data. the data frame created is presumed to be the same as the GeoPoint output
    df_fc = pd.read_csv(os.path.join(os.path.dirname(__file__), 'forecast_data.csv'), index_col=0)

    # dump results of the load
    print(df_nc.head())

    # join the results
    df_join = df_nc.join(df_fc, how='outer')

    # dump results of the join
    print(df_join.head())

    # output the data in a CSV file
    df_join.to_csv(os.path.join(os.path.dirname(__file__), 'nc_fc_joined.csv'))
