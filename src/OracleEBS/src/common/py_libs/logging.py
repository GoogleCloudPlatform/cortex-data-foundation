# Copyright 2022 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""Logging initialization functions."""

import logging
import sys


def initialize_console_logging(debug: bool = False):
    """Initializes Python root logger making sure Debug and Info
    messages go to STDOUT, while WARNING and above go to STDERR

    Args:
        message_only (bool, optional): True if only need to log messages.
            If False, the logging format will be
            "%(asctime)s | %(levelname)s | %(message)s"
        debug (bool, optional): True if logging level must be set to DEBUG.
            It will be set to INFO otherwise. Defaults to False.
    """
    h_info_and_below = logging.StreamHandler(sys.stdout)
    h_info_and_below.setLevel(logging.DEBUG)
    h_info_and_below.addFilter(lambda record: record.levelno <= logging.INFO)
    h_warn_and_above = logging.StreamHandler(sys.stderr)
    h_warn_and_above.setLevel(logging.WARNING)

    handlers = [h_info_and_below, h_warn_and_above]
    log_format = "%(asctime)s | %(levelname)s | %(message)s"
    logging.basicConfig(format=log_format,
                        level=logging.DEBUG if debug else logging.INFO,
                        handlers=handlers,
                        force=True)
