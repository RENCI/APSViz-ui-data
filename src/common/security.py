# SPDX-FileCopyrightText: 2022 Renaissance Computing Institute. All rights reserved.
# SPDX-FileCopyrightText: 2023 Renaissance Computing Institute. All rights reserved.
#
# SPDX-License-Identifier: GPL-3.0-or-later
# SPDX-License-Identifier: LicenseRef-RENCI
# SPDX-License-Identifier: MIT

"""
    Security utilities.

    Author: Phil Owen, 6/27/2023
"""
import os
import jwt

from pydantic import BaseModel, Field


class BearerSchema(BaseModel):
    """
    declare a data model for the Bearer details
    """
    bearer_name: str = Field(...)
    bearer_secret: str = Field(...)

    class Config:
        """
        an example usage of the model
        """
        json_schema_extra = {"bearer_name": "SomeBearerName", "bearer_secret": "SomeBearerSecret"}


class Security:
    """
    Methods to handle security

    """

    def __init__(self):
        """
        Init this class with the JWT params

        """
        self.bearer_name = os.environ.get('BEARER_NAME')
        self.bearer_secret = os.environ.get('BEARER_SECRET')
        self.jwt_algorithm = os.environ.get('JWT_ALGORITHM')
        self.jwt_secret = os.environ.get('JWT_SECRET')

    def sign_jwt(self, token_def: dict):
        """
        creates and returns a signed token

        :return:
        """
        # create the jwt token
        jwt_token = jwt.encode(token_def, self.jwt_secret, algorithm=self.jwt_algorithm)

        # return the new token
        return {"access_token": jwt_token}

    def decode_jwt(self, token: str) -> bool:
        """
        decodes and validates the JWT token

        :param token:
        :return:
        """
        # init the return
        ret_val: bool = False

        try:
            # try to decode the token passed
            decoded_token = jwt.decode(token, self.jwt_secret, algorithms=[self.jwt_algorithm])

            # verify that the token is legit
            if 'bearer_name' in decoded_token and decoded_token['bearer_name'] == self.bearer_name and 'bearer_secret' in decoded_token and \
                    decoded_token['bearer_secret'] == self.bearer_secret:
                ret_val = True

        except Exception:
            # trap a decode error
            ret_val = False

        # return to the caller
        return ret_val
