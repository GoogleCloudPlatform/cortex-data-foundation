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
"""Ad Groups to Product Hierarchy matcher."""

# pylint: disable=expression-not-assigned,no-value-for-parameter

import functools
import typing

import apache_beam as beam
from apache_beam.ml.inference.base import RunInference
from apache_beam.pvalue import AsIter

from pipeline_job.bi_encoder_model_handler import BiEncoderModelHandler
from pipeline_job.cross_encoder_model_handler import CrossEncoderModelHandler
from pipeline_job.model_postprocessing import CalculateDotScore

_TOP_K = 5
_MIN_SCORE_WEIGHT = 0.95


class Matcher(beam.PTransform):
    """DoFn for calculating similarity of product hierarchies for ad groups.

    It expects a PCollection tuple
        with the first element being product hierarchy embeddings
            as received from RunInference(BiEncoderModelHandler()),
        and the second element Ad Groups data rows filtered on is_negative flag.
        !!! Each Ad Group is supposed to have a single entry !!!

    It calculates matches,
      then keeps only matches with scores >= _MIN_SCORE_WEIGHT * top_score,
      but not more than _TOP_K elements.

    It returns a collection of Ad Groups
        with matching product hierarchies.
    Each element is a tuple:
        (
            ag_group_id,
            [
                matching prod_h 1,
                matching prod_h 2,
                ...
            ]
        )
    """

    def expand(self, input_or_inputs) -> beam.pvalue.PCollection:
        prod_hier_embeddings = input_or_inputs[0]
        ad_groups_data = input_or_inputs[1]

        # yapf: disable
        ad_group_texts = (
            ad_groups_data
            | "Extract ad_group_id and ad_group_text" >>
                beam.Map(lambda elem:
                                (elem["ad_group_id"],
                                elem["ad_group_text"])))
        ad_group_embeddings = (
            ad_group_texts |
            "Run Bi-Encoder Inference on Ad Group Texts" >>
                RunInference(BiEncoderModelHandler()))
        ad_groups_scored_with_hierarchies = (
            ad_group_embeddings
            | "Calculate Similarities" >>
                beam.ParDo(CalculateDotScore(),
                    product_hier_embeddings=AsIter(prod_hier_embeddings)) )
        top_k_per_ad_group = (
            ad_groups_scored_with_hierarchies
            | f"Top {_TOP_K} matches per Ad Group" >>
                beam.combiners.Top.PerKey(n=_TOP_K,key=lambda g: g["score"])
            )

        # Now, it's just _TOP_K elements per ad_group, so we can simply
        # iterate over collections
        top_by_weights_per_ad_group = (
            top_k_per_ad_group | "Get top scored" >>
            beam.Map(Matcher._get_top_scored_matches,
                     top_score_ratio=_MIN_SCORE_WEIGHT))

        ad_group_final_matches = (
            top_by_weights_per_ad_group |
            "Run Cross-Encoder for Re-ranking" >>
                RunInference(CrossEncoderModelHandler()))

        ad_group_matches_clean = (
            ad_group_final_matches
            | "Cleanup matches"
            >> beam.Map(Matcher._cleanup_matches)
        )

        return ad_group_matches_clean
        # yapf: enable

    @staticmethod
    def _cleanup_matches(
        ad_group_matches: typing.Tuple[str, typing.Sequence[typing.Dict[str,
                                                                        any]]]):
        # Only one data entry per ad group
        ad_group_id = ad_group_matches[0]
        matches = ad_group_matches[1]

        # Remove redundant attributes
        clean_matches = []
        for m in matches:
            clean_matches.append({
                "prodh": m["prodh"],
                "fullhier": m["fullhier"]
            })
        return (ad_group_id, clean_matches)

    # Must be a separate function due to pickling issues
    @staticmethod
    def _is_good_score(match_dict, min_score):
        return match_dict["score"] > min_score

    @staticmethod
    def _get_top_scored_matches(
        top_k_matches: typing.Tuple[str, typing.Sequence[typing.Dict[str,
                                                                     any]]],
        top_score_ratio: float
    ) -> typing.Tuple[str, typing.Sequence[typing.Dict[str, any]]]:
        ad_group_id = top_k_matches[0]
        matches = top_k_matches[1]
        if len(matches) == 0:
            return (ad_group_id, [])
        sorted_matches = sorted(matches, key=lambda m: m["score"], reverse=True)
        max_score = sorted_matches[0]["score"]
        min_score = max_score * top_score_ratio
        good_matches = list(
            filter(
                functools.partial(Matcher._is_good_score, min_score=min_score),
                sorted_matches))
        # Normalize scores
        score_sum = sum([m["score"] for m in good_matches])
        for m in good_matches:
            m["score"] = m["score"] / score_sum
        return (ad_group_id, good_matches)
