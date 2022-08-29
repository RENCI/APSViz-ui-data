# SPDX-FileCopyrightText: 2022 Renaissance Computing Institute. All rights reserved.
#
# SPDX-License-Identifier: GPL-3.0-or-later
# SPDX-License-Identifier: LicenseRef-RENCI
# SPDX-License-Identifier: MIT

# This Dockerfile is used to build THE apsviz-ui-data python image
# starts with the python image
# creates a directory for the repo
# gets the apsviz-ui-data repo
# and runs main which starts the web server

FROM renciorg/renci-python-image:v0.0.1

# get some credit
LABEL maintainer="powen@renci.org"

# install basic tools
RUN apt-get update

# create log level env param (debug=10, info=20)
ENV LOG_LEVEL 20

# make a directory for the repo
RUN mkdir /repo

# go to the directory where we are going to upload the repo
WORKDIR /repo

# get the latest code
RUN git clone https://github.com/RENCI/apsviz-ui-data.git

# go to the repo dir
WORKDIR /repo/apsviz-ui-data

# make sure everything is read/write in the repo code
RUN chmod 777 -R .

# install requirements
RUN pip install -r requirements.txt

# switch to the non-root user (nru). defined in the base image
USER nru

# expose the default port
EXPOSE 4000

# start the service entry point
ENTRYPOINT ["python", "main.py"]