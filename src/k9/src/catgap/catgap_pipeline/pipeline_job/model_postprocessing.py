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
"""Postprocessing models results."""

import typing

import apache_beam as beam
from apache_beam.ml.inference.base import PredictionResult


class CalculateDotScore(beam.DoFn):
    """DoFn for calculating similarity of product hierarchies for ad groups"""

    def process(self, element, product_hier_embeddings):
        return CalculateDotScore.calculate_dot_scores(element,
                                                      product_hier_embeddings)

    @staticmethod
    def calculate_dot_scores(
        ad_group_embeddings: PredictionResult,
        product_hier_embeddings: typing.Iterable[PredictionResult]
    ) -> typing.Iterable[typing.Tuple[str, typing.Dict[str, any]]]:
        """Calculates similarity of product hierarchies for ad groups

        Args:
            ad_group_embeddings (PredictionResult):
                PredictionResult as a tuple of
                    [(ad_group_id, text), embeddings_vector]
            product_hier_embeddings(typing.Iterable[PredictionResult]):
                list of PredictionResult as tuples of
                    [(prod_hier_id, prod_hier_text), embeddings_vector]

        Returns:
            typing.Iterable[typing.Tuple[str, typing.Dict[str, any]]]:
                an iterable of 2-element tuples made as
                 ( ad_group_id,
                        dict("ad_group_id": ...,
                             "ad_group_text": ...,
                             "prodh": ...,
                             "fullhier": ...,
                             "score": ...))
        """

        ad_group_tuple = ad_group_embeddings[0]
        ad_group_id = ad_group_tuple[0]
        ad_group_text = ad_group_tuple[1]
        ads_embeddings_vector = ad_group_embeddings[1]
        for prod_emb in product_hier_embeddings:
            prod_emb_tuple = prod_emb[0]
            prod_emb_vector = prod_emb[1]
            score = 0
            for i, emb in enumerate(prod_emb_vector):
                score += emb * ads_embeddings_vector[i]
            yield (ad_group_id, {
                "ad_group_id": ad_group_id,
                "ad_group_text": ad_group_text,
                "prodh": prod_emb_tuple[0],
                "fullhier": prod_emb_tuple[1],
                "score": score
            })
