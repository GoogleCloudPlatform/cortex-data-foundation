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
"""Beam pipeline utils."""

import apache_beam as beam


def unnest(input_element,
           sub_elements_to_unnest_as_list=None,
           sub_elements_to_unnest_as_values=None):
    """This function un-nests nested dictionaries made by CoGoupByKey"""
    value = input_element[1]
    if sub_elements_to_unnest_as_list:
        for sub_element in sub_elements_to_unnest_as_list:
            value[sub_element] = (value[sub_element][0]
                                  if hasattr(value[sub_element], "__len__") and
                                  len(value[sub_element]) > 0 else
                                  value[sub_element])
    if sub_elements_to_unnest_as_values:
        for sub_element in sub_elements_to_unnest_as_values:
            value[sub_element] = (value[sub_element][0]
                                  if hasattr(value[sub_element], "__len__") and
                                  len(value[sub_element]) > 0 else None)
    return (input_element[0], value)


class DoNothing(beam.PTransform):
    """This transform does nothing, and intended for controlling
    operations dependencies.
    It returns original inputs as they are.
    """

    def expand(self, input_or_inputs):
        return input_or_inputs
