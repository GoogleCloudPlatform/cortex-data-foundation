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
"""A lightweight BigQuery reading component.
Made as a workaround for b/273265766.
"""
from google.cloud import bigquery

import apache_beam as beam
from apache_beam.options.pipeline_options import GoogleCloudOptions

from shared.auth import get_auth


class ReadFromBigQueryLight(beam.PTransform):
    """BigQuery data reader for small results.
    It supports external tables with connected spreadsheets.
    Made as a workaround for b/273265766.
    """

    def __init__(self, query, location=None, label=None):
        self.query = str(query)
        self.location = str(location)
        super().__init__(label=label)

    def expand(self, input_or_inputs):
        gcp_options = input_or_inputs.pipeline.options.view_as(
            GoogleCloudOptions)
        return input_or_inputs.pipeline | beam.io.Read(
            _ReadFromBigQueryLightSource(self.query, self.location,
                                         gcp_options))


class _ReadFromBigQueryLightSource(beam.io.iobase.BoundedSource):
    """Data source for ReadModifiedAdGroups.
    Executes a BigQuery query directly.
    """

    def __init__(self, query, location, gcp_options):
        self.query = query
        self.location = location
        self.gcp_options = gcp_options

    def estimate_size(self):
        return None

    def get_range_tracker(self, start_position, stop_position):
        """Implements
            :class:`~apache_beam.io.iobase.BoundedSource.get_range_tracker`"""
        if start_position is None:
            start_position = 0
        if stop_position is None:
            stop_position = (
                beam.io.range_trackers.OffsetRangeTracker.OFFSET_INFINITY)

        # Use an unsplittable range tracker.
        # Result collections can only be read sequentially.
        # It is ok with our case as the size of collection
        # will be a few thousands at most.
        range_tracker = beam.io.range_trackers.OffsetRangeTracker(
            start_position, stop_position)
        range_tracker = beam.io.range_trackers.UnsplittableRangeTracker(
            range_tracker)

        return range_tracker

    def read(self, range_tracker):
        del range_tracker
        return self._query()

    def split(self,
              desired_bundle_size,
              start_position=None,
              stop_position=None):
        """Implements :class:`~apache_beam.io.iobase.BoundedSource.split`
        This function will currently not be called, because the range tracker
        is unsplittable
        """
        del desired_bundle_size
        if start_position is None:
            start_position = 0
        if stop_position is None:
            stop_position = (
                beam.io.range_trackers.OffsetRangeTracker.OFFSET_INFINITY)

        # Because the source is unsplittable (for now), only a single source is
        # returned.
        yield beam.io.iobase.SourceBundle(weight=1,
                                          source=self,
                                          start_position=start_position,
                                          stop_position=stop_position)

    def _query(self):
        service_account = (self.gcp_options.service_account_email or
                           self.gcp_options.impersonate_service_account)
        credentials = get_auth(project_id=self.gcp_options.project,
                               service_account_principal=service_account)
        job = bigquery.Client(project=self.gcp_options.project,
                              location=self.location,
                              credentials=credentials).query(self.query)
        for row in job:
            yield dict(row.items())
