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
"""Final transform to write all the matches."""

# pylint: disable=expression-not-assigned,no-value-for-parameter

import datetime
import typing

import apache_beam as beam
from apache_beam.pvalue import AsList
from apache_beam.transforms.util import BatchElements
from apache_beam.options.pipeline_options import GoogleCloudOptions

import gspread

from shared.auth import get_auth
from shared.sheets import (get_mapping_journal_hier_sheets,
                           prepare_mapping_spreadsheet, finish_spreadsheet,
                           PLUS_SIGN, MINUS_SIGN)
from pipeline_job.pipeline_options import CatgapOptions
from pipeline_job.read_from_bq import ReadFromBigQueryLight

_SPREADSHEET_BATCH_ROWS_COUNT = 1000


class MatchesWriter(beam.PTransform):
    """This transform writes Ad Group matching data to the target spreadsheet
    and to the log table (ads_to_sap_prod_hier_log).
    It keeps unmodified all Ad Groups mappings that were previously
    modified manually.
    """

    def expand(self, input_or_inputs):
        self.gcp_options = input_or_inputs.pipeline.options.view_as(
            GoogleCloudOptions)
        self.catgap_options = input_or_inputs.pipeline.options.view_as(
            CatgapOptions)

        # First, get data of ad group mapping that were manually modified.
        ads_groups_with_modifications = (
            f"`{self.catgap_options.source_project}."
            f"{self.catgap_options.k9_processing_dataset}."
            "ads_to_sap_modified_mappings`")
        ads_to_sap_prod_hier = (f"`{self.catgap_options.source_project}."
                                f"{self.catgap_options.k9_processing_dataset}."
                                "ads_to_sap_prod_hier`")
        in_query = f"SELECT ad_group_id FROM {ads_groups_with_modifications}"
        query = (f"SELECT * FROM {ads_to_sap_prod_hier} WHERE "
                 f"ad_group_id IN ({in_query})")
        modified_ad_group_mappings = (input_or_inputs | ReadFromBigQueryLight(
            query=query,
            location=self.catgap_options.bigquery_location) | beam.Map(
                lambda m: {
                    "campaign_name": m["campaign_name"],
                    "ad_group_name": m["ad_group_name"],
                    "is_negative": m["is_negative"],
                    "fullhier": m["fullhier"]
                }))

        # Next, expand new mapping data into the same plain format,
        # while filtering out ad groups with mappings that were
        # previously modified.

        # New or updated matches - these will be written to the spreadsheet
        # along with the previously manually modified matches
        # (modified_ad_group_mappings).
        new_mappings = (input_or_inputs |
                        beam.Filter(lambda m: m[1]["is_modified"] is not True) |
                        beam.Map(MatchesWriter._resolve_conflicts) |
                        beam.ParDo(_FlattenMatches()))

        # We can concatenate new_mappings and modified_ad_group_mappings,
        # and convert to a list of spreadsheet rows
        to_write_to_spreadsheet = (
            (modified_ad_group_mappings, new_mappings) | beam.Flatten() |
            beam.Map(MatchesWriter._convert_is_negative) | beam.Map(lambda m: [
                m["campaign_name"], m["ad_group_name"], m["is_negative"], m[
                    "fullhier"]
            ]))
        # Group mappings to batches of _SPREADSHEET_BATCH_ROWS_COUNT rows.
        batches_to_write = to_write_to_spreadsheet | BatchElements(
            _SPREADSHEET_BATCH_ROWS_COUNT)

        mapping_timestamp = datetime.datetime.utcnow()
        timestamp_1_list = batches_to_write.pipeline | beam.Create(
            [mapping_timestamp])
        timestamp_1_list = timestamp_1_list | beam.Map(
            self._prepare_spreadsheet)
        written_spreadsheet_batches = batches_to_write | beam.Map(
            self._write_to_spreadsheet, AsList(timestamp_1_list))
        # Finalizing the spreadshit.
        # Accepting written_spreadsheet_batches makes it run after
        # the previous step.
        timestamp_1_list = timestamp_1_list | beam.Map(
            self._finish_spreadsheet, AsList(written_spreadsheet_batches))

        # All mappings - these should be written to ads_to_sap_prod_hier_log.
        # !!! WE CAN ONLY WRITE IT to ads_to_sap_prod_hier_log _AFTER_
        # the spreadsheet IS DONE !!!
        # Otherwise it will mess up `ads_to_sap_modified_mappings` logic.
        mappings = (input_or_inputs | beam.Values() |
                    beam.ParDo(_FlattenMatchesForLog()) |
                    beam.Map(lambda m: MatchesWriter._add_mapping_timestamp(
                        m, mapping_timestamp)))
        # To avoid conflicts, we run a dummy transform that accepts
        # a materialized list of timestamp_1_list.
        mappings = mappings | beam.Map(lambda m, _: m, AsList(timestamp_1_list))

        mappings | beam.io.WriteToBigQuery(
            table="ads_to_sap_prod_hier_log",
            dataset=str(self.catgap_options.k9_processing_dataset),
            project=str(self.catgap_options.source_project))

        return mappings

    @staticmethod
    def _get_temp_sheet(original_ws):
        spreadsheet = original_ws.spreadsheet
        temp_name = original_ws.title + " - TEMP"
        temp_list = list(
            filter(lambda ws: ws.title == temp_name, spreadsheet.worksheets()))
        if len(temp_list) > 0:
            return temp_list[0]
        else:
            return original_ws.duplicate(new_sheet_name=temp_name)

    def _prepare_spreadsheet(self, arg):
        """This Will only be executed once be mapping session.
        Returns arg as it is.
        """
        service_account = str(self.gcp_options.service_account_email or
                              self.gcp_options.impersonate_service_account)
        prepare_mapping_spreadsheet(
            str(self.catgap_options.mapping_spreadsheet),
            str(self.catgap_options.source_project),
            str(self.catgap_options.k9_processing_dataset),
            str(self.catgap_options.bigquery_location), str(service_account))

        credentials = get_auth(project_id=str(self.gcp_options.project),
                               service_account_principal=service_account)

        gs_client = gspread.Client(credentials)
        spreadsheet = gs_client.open_by_url(
            str(self.catgap_options.mapping_spreadsheet))
        ws_mapping, _, _ = get_mapping_journal_hier_sheets(spreadsheet)

        ws_mapping = MatchesWriter._get_temp_sheet(ws_mapping)

        # Remove all existing rows in mapping, except header and first row
        row_num = ws_mapping.row_count
        if row_num > 2:
            ws_mapping.delete_rows(2, row_num - 1)
        # clear first row
        cell_list = ws_mapping.range("A2:D2")
        for cell in cell_list:
            cell.value = ""
        ws_mapping.update_cells(cell_list)
        return arg

    def _finish_spreadsheet(self, mapping_timestamp, _):
        """This Will only be executed once be mapping session.
        Returns arg as it is.
        """
        service_account = str(self.gcp_options.service_account_email or
                              self.gcp_options.impersonate_service_account)
        prepare_mapping_spreadsheet(
            str(self.catgap_options.mapping_spreadsheet),
            str(self.catgap_options.source_project),
            str(self.catgap_options.k9_processing_dataset),
            str(self.catgap_options.bigquery_location), str(service_account))

        credentials = get_auth(project_id=str(self.gcp_options.project),
                               service_account_principal=service_account)

        gs_client = gspread.Client(credentials)
        spreadsheet = gs_client.open_by_url(
            str(self.catgap_options.mapping_spreadsheet))
        ws_mapping, ws_journal, ws_hier = get_mapping_journal_hier_sheets(
            spreadsheet)

        ws_mapping_temp = MatchesWriter._get_temp_sheet(ws_mapping)

        # clear first row if it's empty
        row_num = ws_mapping_temp.row_count
        if row_num > 2:
            cell_list = ws_mapping_temp.range("A2:D2")
            combined_value = ""
            for cell in cell_list:
                combined_value += cell.value
            if combined_value == "":
                ws_mapping_temp.delete_row(1)

        ws_mapping_title = ws_mapping.title
        spreadsheet.del_worksheet(ws_mapping)
        ws_mapping_temp.update_title(ws_mapping_title)
        ws_mapping_temp.update_index(0)
        ws_mapping = ws_mapping_temp

        finish_spreadsheet(ws_mapping, ws_journal, ws_hier)
        return mapping_timestamp

    def _write_to_spreadsheet(
            self, mapping_data: typing.Iterable[typing.Iterable[str]],
            mapping_timestamp_1_element: typing.List[datetime.datetime]):
        service_account = str(self.gcp_options.service_account_email or
                              self.gcp_options.impersonate_service_account)
        credentials = get_auth(project_id=self.gcp_options.project,
                               service_account_principal=service_account)

        gs_client = gspread.Client(credentials)
        spreadsheet = gs_client.open_by_url(
            str(self.catgap_options.mapping_spreadsheet))
        ws_mapping, ws_journal, _ = get_mapping_journal_hier_sheets(spreadsheet)

        ws_mapping = MatchesWriter._get_temp_sheet(ws_mapping)

        ws_mapping.append_rows(mapping_data)

        ts = mapping_timestamp_1_element[0].date().isoformat()
        unique_pairs = {(r[0], r[1]) for r in mapping_data}
        journal_rows = [[ts, r[0], r[1]] for r in unique_pairs]
        ws_journal.append_rows(journal_rows)
        return True

    @staticmethod
    def _resolve_conflicts(element):
        """Resolves conflicts between positive and negative matches.
        If a negative match is on the same or higher level of the hierarchy
        as any positive match, such negative match will be "emptied".
        A row with an empty SAP category column will signal that
        the model was not able to find a correct negative match.
        """
        positive_hiers = [
            match["fullhier"]
            for match in element[1]["matches"]["positive_matches"]
        ]
        for match in element[1]["matches"]["negative_matches"]:
            fh = match["fullhier"]
            for ph in positive_hiers:
                if ph.startswith(fh):
                    match["fullhier"] = ""
                    match["prodh"] = ""
        return element

    @staticmethod
    def _convert_is_negative(row):
        """Converts is_negative into ✚/━

        Args:
            row (dict): original value

        Yields:
            dict: converted value
        """

        row["is_negative"] = MINUS_SIGN if row["is_negative"] else PLUS_SIGN
        return row

    @staticmethod
    def _add_mapping_timestamp(row, mapping_timestamp):
        """Adds mapping_timestamp value to a dict
        """

        row["mapping_timestamp"] = mapping_timestamp
        return row


class _FlattenMatches(beam.DoFn):
    """Convert Ad Group matching data to a flat list
    to be written to the mapping spreadsheet."""

    def process(self, element):
        data = element[1]["data"]
        for match in element[1]["matches"]["positive_matches"]:
            yield {
                "campaign_name": data["campaign_name"],
                "ad_group_name": data["ad_group_name"],
                "is_negative": False,
                "fullhier": match["fullhier"]
            }
        for match in element[1]["matches"]["negative_matches"]:
            yield {
                "campaign_name": data["campaign_name"],
                "ad_group_name": data["ad_group_name"],
                "is_negative": True,
                "fullhier": match["fullhier"]
            }


class _FlattenMatchesForLog(beam.DoFn):
    """Convert Ad Group matching data to a flat list
    to be written to the mapping log."""

    def process(self, element):
        data = element["data"]
        for match in element["matches"]["positive_matches"]:
            yield {
                "campaign_id": data["campaign_id"],
                "ad_group_id": data["ad_group_id"],
                "is_negative": False,
                "prodh": match["prodh"]
            }
        for match in element["matches"]["negative_matches"]:
            yield {
                "campaign_id": data["campaign_id"],
                "ad_group_id": data["ad_group_id"],
                "is_negative": True,
                "prodh": match["prodh"]
            }
