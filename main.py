# SPDX-FileCopyrightText: 2022 Renaissance Computing Institute. All rights reserved.
#
# SPDX-License-Identifier: GPL-3.0-or-later
# SPDX-License-Identifier: LicenseRef-RENCI
# SPDX-License-Identifier: MIT

import uvicorn


class App:
    ...


app = App()

if __name__ == "__main__":
    uvicorn.run("src.server:APP", host="0.0.0.0", port=4000, log_level="info", workers=1)
