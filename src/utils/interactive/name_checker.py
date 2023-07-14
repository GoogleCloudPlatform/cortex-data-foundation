# Copyright 2022 Google LLC

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#     https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Cloud objects name validity checkers"""

import re

DATASET_EXP = "^[a-zA-Z0-9_]{1,1023}$"
BUCKET_EXP = r"^[a-z0-9][-a-z0-9_.]{2,63}$|^[a-z0-9][-a-z0-9_.]{1,63}"\
             r"(\.[a-z0-9][-a-z0-9_.]{3,63}){1,31}$"

def is_dataset_name_valid(name: str) -> bool:
    """Checks validity of BigQuery dataset name

    Args:
        name (str): dataset name

    Returns:
        bool: True if valid, False otherwise
    """
    return re.match(DATASET_EXP, name) is not None


def is_bucket_name_valid(name: str) -> bool:
    """Checks validity of Cloud Storage Bucket name

    Args:
        name (str): bucket name

    Returns:
        bool: True if valid, False otherwise
    """
    return re.match(BUCKET_EXP, name) is not None
