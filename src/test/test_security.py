# SPDX-FileCopyrightText: 2022 Renaissance Computing Institute. All rights reserved.
# SPDX-FileCopyrightText: 2023 Renaissance Computing Institute. All rights reserved.
#
# SPDX-License-Identifier: GPL-3.0-or-later
# SPDX-License-Identifier: LicenseRef-RENCI
# SPDX-License-Identifier: MIT

"""
    Settings tests.

    Author: Phil Owen, 6/27/2023
"""
import os

import requests
import pytest

from src.common.security import Security


@pytest.mark.skip(reason="Local test only")
def test_sign_jwt():
    """
    tests the creation of a JWT token

    :return:
    """
    # create a security object
    sec = Security()

    # create a payload for the token generation
    payload = {'bearer_name': os.environ.get("BEARER_NAME"), 'bearer_secret': os.environ.get("BEARER_SECRET")}

    # create a new token
    token = sec.sign_jwt(payload)

    # check the result
    assert token and 'access_token' in token


@pytest.mark.skip(reason="Local test only")
def test_decode_jwt():
    """
    tests the decode and validation of a JWT token

    :return:
    """
    # create a security object
    sec = Security()

    # create a payload for the token generation
    payload = {'bearer_name': os.environ.get("BEARER_NAME"), 'bearer_secret': os.environ.get("BEARER_SECRET")}

    # create a new token
    token = sec.sign_jwt(payload)

    # decode the jwt token
    ret_val = sec.decode_jwt(token['access_token'])

    # validate the result
    assert ret_val

    # decode the jwt token
    ret_val = sec.decode_jwt(token['access_token'] + 'this-will-fail')

    assert not ret_val


@pytest.mark.skip(reason="Local test only")
def test_access():
    """
    makes a secure request to the app running locally

    :return:
    """
    # create a security object
    sec = Security()

    # create a pyload for the token generation
    payload = {'bearer_name': os.environ.get("BEARER_NAME"), 'bearer_secret': os.environ.get("BEARER_SECRET")}

    # create a new token
    token = sec.sign_jwt(payload)

    # create an auth header
    auth_header: dict = {'Content-Type': 'application/json', 'Authorization': f'Bearer {token["access_token"]}'}

    # execute the post
    ret_val = requests.get('http://localhost:4000/get_ui_data_secure?met_class=synoptic&limit=2', headers=auth_header, timeout=10)

    # was the call unsuccessful
    assert ret_val.status_code == 200
