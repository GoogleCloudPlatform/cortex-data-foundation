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
"""Required for multi file Beam pipelines."""

from setuptools import find_packages
from setuptools import setup

setup(name="TikTokBeamDependencies",
      version="0.1",
      description="Python Apache Beam pipeline dependencies",
      install_requires=["requests==2.32.3",
                        "google-cloud-secret-manager==2.17.0"],
      author="Google Cloud Cortex",
      author_email="cortex-support@google.com",
      url="https://cloud.google.com/solutions/cortex",
      packages=find_packages())
