# Copyright 2023 Google LLC

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#     https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Data Foundation Deployment UI constants"""

DF_TITLE = ("Google <pp fg='#4285F4'>Cloud</pp> <pp fg='#EA4335'>Cortex</pp>"
          " <pp fg='#FBBC05'>Data</pp> <pp fg='#34A853'>Foundation</pp>")

CONFIG_MESSAGE = (
    "Source Project <b>%s</b>.\nTarget Project: <b>%s</b>.\n"
    "Location: <b>%s</b>.\n"
    "APIs that will be enabled:\n"
    "    %s\n\n"
    "Following resources will be created if don't exist:\n"
    "    BigQuery Datasets: <b>%s</b>\n"
    "    Storage Buckets: <b>%s</b>"
    "\n\n"
    "Cloud Build Account of project <b>%s</b> "
    "will be given the following role assignments:\n"
    "    <b>%s</b> in project(s) <b>%s</b>\n"
    "    <b>%s</b> for BigQuery datasets above\n"
    "    <b>%s</b> for Storage Buckets above"
)
