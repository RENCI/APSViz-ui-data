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
from enum import Enum


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
    def handle_instance_name(request_type: str, instance_name: Enum, reset: bool):
        """
        handles the instance name request

        :param request_type:
        :param instance_name:
        :param reset:
        :return:
        """
        # set the apsviz file path
        file_path: str = os.getenv('INSTANCE_NAME_FILE_PATH', '') + request_type

        # if this was a apsviz operation
        if reset and os.path.exists(file_path):
            # delete the config file
            os.remove(file_path)

            # return the reset operation succeeded
            ret_val = f'{request_type} reset operation performed'

        # set the apsviz instance name
        elif instance_name is not None:
            # open the config file for writing
            with open(file_path, 'w', encoding='utf-8') as fp:
                fp.write(instance_name.value)

                # save the instance name
                ret_val = f'{request_type} instance name set to: {instance_name}'
        elif instance_name is None and os.path.exists(file_path):
            # open the config file for reading
            with open(file_path, 'r', encoding='utf-8') as fp:
                # save the instance name in the file
                ret_val = fp.read()

                # if we encountered an empty file
                if ret_val == '':
                    ret_val = "Error: No value found in storage."
        else:
            ret_val = f'Error: No {request_type} operation performed'

        # return the result to the caller
        return ret_val

    @staticmethod
    def cleanup(file_path: str):
        """
        removes the directory from the file system

        :param file_path:
        :return:
        """
        shutil.rmtree(file_path)


class BrandName(str, Enum):
    """
    Class enum for k8s job type names
    """
    APSVIZ = 'APSViz'
    NOPP = 'NOPP'
