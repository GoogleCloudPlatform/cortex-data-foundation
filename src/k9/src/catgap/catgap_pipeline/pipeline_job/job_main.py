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
"""SAP Hierarchy to Google Ads Mapping - main module."""

# pylint: disable=expression-not-assigned,no-value-for-parameter

import typing
import warnings

import apache_beam as beam
from apache_beam.utils.annotations import BeamDeprecationWarning
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions
from apache_beam.ml.inference.base import RunInference

from pipeline_job.pipeline_options import CatgapOptions
from pipeline_job.bi_encoder_model_handler import BiEncoderModelHandler
from pipeline_job.matcher import Matcher
from pipeline_job.read_modified_ad_groups import ReadModifiedAdGroups
from pipeline_job.utils import unnest
from pipeline_job.matches_writer import MatchesWriter


def pipeline_main(options: typing.Sequence[str]) -> int:
    warnings.filterwarnings("ignore", category=BeamDeprecationWarning)
    pipeline_options = PipelineOptions(list(options))
    catgap_options = pipeline_options.view_as(CatgapOptions)
    gcp_options = pipeline_options.view_as(GoogleCloudOptions)
    gcp_options.impersonate_service_account = gcp_options.service_account_email

    # yapf: disable
    with beam.Pipeline(options=pipeline_options) as pipeline:
        prod_hiers = (pipeline
            | "Get SAP Product Hierarchy" >>
                beam.io.ReadFromBigQuery(
                    use_standard_sql=True,
                    validate=True,
                    query=("SELECT prodh, fullhier FROM "
                          f"`{catgap_options.source_project}."
                          f"{catgap_options.k9_processing_dataset}"
                          ".prod_hierarchy`"))
            | "Pre-process SAP Product Hierarchy" >>
                beam.Map(lambda elem: (elem["prodh"], elem["fullhier"])))
        prod_hier_embeddings = (prod_hiers |
            "Run Bi-Encoder on Product Hierarchies" >>
            RunInference(BiEncoderModelHandler()))

        ads_targeting_criterions_view = (f"`{catgap_options.source_project}."
                                    f"{catgap_options.k9_processing_dataset}."
                                    "ads_targeting_criterions`")
        modified_ad_groups = (pipeline
                              | "Reading manually modified Ad Group mappings"
                              >> ReadModifiedAdGroups())
        modified_ad_groups = (modified_ad_groups
                              | "Make modified ad group collection keyed" >>
                              beam.Map(lambda elem: (str(elem), True)))

        ad_groups_data = (pipeline
            | "Get Ad Group Texts" >>
                beam.io.ReadFromBigQuery(
                    use_standard_sql=True,
                    validate=True,
                    query= f"""SELECT
                        CAST(campaign_id AS STRING) AS campaign_id,
                        campaign_name,
                        CAST(ad_group_id AS STRING) AS ad_group_id,
                        ad_group_name,
                        is_negative,
                        STRING_AGG(REPLACE(TRIM(criterion_text, "/" ),
                            "/", ", "), ". "
                            ORDER BY is_keyword DESC) AS ad_group_text
                            FROM {ads_targeting_criterions_view}
                            WHERE
                                ad_group_status = "ENABLED"
                                AND criterion_status = "ENABLED"
                            GROUP BY campaign_id, campaign_name,
                                ad_group_id, ad_group_name, is_negative"""))

        # ad_groups_data contains table of ad_groups
        # with joined criterion texts.
        # Each ag_group may have 2 entries in this collection:
        #    1. Positive keywords joined with targeting audiences.
        #    2. Negative keywords.
        # The second one has `is_negative` property true.

        ad_groups_data_positives = (
            ad_groups_data | "Retrieve positive criterions" >>
            beam.Filter(lambda elem: elem["is_negative"] is False))
        ad_groups_data_negatives = (
            ad_groups_data | "Retrieve negative criterions" >>
            beam.Filter(lambda elem: elem["is_negative"] is True))

        matches_positives = (
            (prod_hier_embeddings, ad_groups_data_positives) |
                "Find matches for ad groups' positive criterions"
                >> Matcher())
        matches_negatives = (
            (prod_hier_embeddings, ad_groups_data_negatives) |
                "Find matches for ad groups' negative criterions"
                >> Matcher())

        grouped_matches = (
            ({"positive_matches": matches_positives,
              "negative_matches": matches_negatives})
            | "Group all matches by Ad Group" >> beam.CoGroupByKey())

        # grouped_matches have nested arrays for both "*_matches"
        # with the "outer" array containing only one element.
        # Let's un-nest them.
        grouped_matches = (
            grouped_matches | "Un-nesting collections of matches"
            >> beam.Map(unnest, ["positive_matches", "negative_matches"]))

        # Now the collection contains exactly on entry per Ad Group,
        # and each entry has up to 5 positive and up to 5 negative
        # matches

        # Now, get necessary ad_group_data, one row per ad_group.
        # We can get it from initial ad_groups_data_positives
        # because there is at least one positive row for each ad group.
        ad_groups_data = (ad_groups_data_positives
                         | "Extract necessary Ad Group Data"
                         >> beam.Map(
                            lambda elem:
                                (elem["ad_group_id"],
                                 {
                                    "campaign_id": elem["campaign_id"],
                                    "campaign_name": elem["campaign_name"],
                                    "ad_group_id": elem["ad_group_id"],
                                    "ad_group_name": elem["ad_group_name"],
                                 })
                         ))
        # And combine them with matches
        grouped_matches_with_data = (
            ({"data": ad_groups_data,
              "matches": grouped_matches,
              "is_modified": modified_ad_groups})
            | "Combine matches with data and modification info"
            >> beam.CoGroupByKey())
        # As before, CoGroupByKey makes nested arrays, so let's un-nest them
        grouped_matches_with_data = (
            grouped_matches_with_data | "Un-nesting values"
            >> beam.Map(unnest, ["matches"], ["data", "is_modified"]))

        # Now we have something like this:
        # ('142682673796', # ad_group_id
        #   {
        #    'data': {'campaign_id': '18373697715',
        #        'campaign_name': '18373697715',
        #        'ad_group_id': '142682673796',
        #        'ad_group_name': 'Ad group 4'
        #    },
        #    'is_modified': True, (!!! we get None instead of False !!!)
        #    'matches': {
        #      'positive_matches': [
        #       {
        #         'prodh': '202ABCWN',
        #         'fullhier':
        #            'Perishables/Beverages/Alcoholic Beverage/Cymbal/Wine'
        #       } ],
        #      'negative_matches': [
        #      {
        #         'prodh': '202ABCWNWH',
        #         'fullhier':
        #          'Perishables/Beverages/Alcoholic Beverage/Cymbal/Wine/White'
        #       } ]
        #     }
        #  } )

        # Now post-process and write matches to the spreadsheet in BQ

        results = (grouped_matches_with_data
                   | "Post-process and write all matches" >> MatchesWriter())
        results | "Print all matches" >> beam.Map(print)


    # yapf: enable

    return 0
