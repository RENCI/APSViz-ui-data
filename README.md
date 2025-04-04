<!--
SPDX-FileCopyrightText: 2022 Renaissance Computing Institute. All rights reserved.
SPDX-FileCopyrightText: 2023 Renaissance Computing Institute. All rights reserved.
SPDX-FileCopyrightText: 2024 Renaissance Computing Institute. All rights reserved.

SPDX-License-Identifier: GPL-3.0-or-later
SPDX-License-Identifier: LicenseRef-RENCI
SPDX-License-Identifier: MIT
-->

# APSViz-UI-Data
Website/services that return data used to populate the [APSViz TerriaMap UI](https://github.com/RENCI/TerriaMap) as well as provide data access to collaborators.

#### Licenses...
[![MIT License](https://img.shields.io/badge/License-MIT-orange.svg)](https://github.com/RENCI/apsviz-ui-data/tree/master/LICENSE)
[![GPLv3 License](https://img.shields.io/badge/License-GPL%20v3-yellow.svg)](https://opensource.org/licenses/)
[![RENCI License](https://img.shields.io/badge/License-RENCI-blue.svg)](https://www.renci.org/)
#### Components and versions...
[![Python](https://img.shields.io/badge/Python-3.13.2-orange)](https://github.com/python/cpython)
[![Linting Pylint](https://img.shields.io/badge/Pylint-3.3.5-yellow)](https://github.com/PyCQA/pylint)
[![Pytest](https://img.shields.io/badge/Pytest-8.3.5-blue)](https://github.com/pytest-dev/pytest)
#### Build status..
[![PyLint the codebase](https://github.com/RENCI/apsviz-ui-data/actions/workflows/pylint.yml/badge.svg)](https://github.com/RENCI/apsviz-ui-data/actions/workflows/pylint.yml)
[![Build and push the Docker image](https://github.com/RENCI/apsviz-ui-data/actions/workflows/image-push.yml/badge.svg)](https://github.com/RENCI/apsviz-ui-data/actions/workflows/image-push.yml)

## Description
This product utilizes a FASTAPI interface to provide data to the [APSViz TerriaMap UI](https://github.com/RENCI/TerriaMap) as well as some internal projects and external collaborators.

There are GitHub actions to maintain code quality in this repo:
 - Pylint (minimum score of 10/10 to pass),
 - Build/publish a Docker image.

Helm/k8s charts for this product are available at: [APSViz-Helm](https://github.com/RENCI/apsviz-helm/tree/main/ui-data).
