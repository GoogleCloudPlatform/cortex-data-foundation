# Copyright 2024 Google LLC
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
"""Constants for Cortex Data Foundation."""

# Listed first due to dependencies
CORTEX_VERSION = "6.2"

# Version formatted with X_X due to BigQuery dataset naming requirements
TEST_HARNESS_VERSION = "6_2"

# Dict for dataset labels
BQ_DATASET_LABEL = {"goog-packaged-solution" : "cortex-framework"}

# Path to the .env file that is written by Cloud Build and
# contains the config file absolute path
CONFIG_FILE_FULL_PATH_ENV = "/workspace/config_file_full_path.env"

# Dict for Cortex job labels
CORTEX_JOB_LABEL = {"requestor": "cortex_runtime"}

# Cortex BigQuery User agent
CORTEX_USER_AGENT = f"cortex/{CORTEX_VERSION} (GPN:Google-Cloud-Cortex;)"

# Default Gemini model for Cross-Media
K9_CROSS_MEDIA_DEFAULT_TEXT_GENERATION_MODEL = "gemini-2.0-flash-001"
