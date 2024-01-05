# SPDX-FileCopyrightText: 2022 Renaissance Computing Institute. All rights reserved.
# SPDX-FileCopyrightText: 2023 Renaissance Computing Institute. All rights reserved.
# SPDX-FileCopyrightText: 2024 Renaissance Computing Institute. All rights reserved.
#
# SPDX-License-Identifier: GPL-3.0-or-later
# SPDX-License-Identifier: LicenseRef-RENCI
# SPDX-License-Identifier: MIT

"""
    General utilities.

    Author: Phil Owen, 6/27/2023
"""

import os
import shutil


class GenUtils:
    """
    General utilities

    """
    @staticmethod
    def filter_catalog_past_runs(catalog_data: dict) -> dict:
        """
        filters out the non-PSC past run data

        :param catalog_data:
        :return:
        """
        # make sure we have something to filter
        if catalog_data['past_runs'] is not None:
            # get the PSC project list
            psc_sync_projects: list = os.environ.get('PSC_SYNC_PROJECTS').split(',')

            # filter out non-PSC data from the past_runs
            catalog_data['past_runs'] = list(filter(lambda item: (item['project_code'] in psc_sync_projects), catalog_data['past_runs']))

        # return to the caller
        return catalog_data

    @staticmethod
    def cleanup(file_path: str):
        """
        removes the directory from the file system

        :param file_path:
        :return:
        """
        shutil.rmtree(file_path)
