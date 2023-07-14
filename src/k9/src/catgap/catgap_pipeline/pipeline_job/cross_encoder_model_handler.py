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
"""Semantic similarity Cross-Encoder Model Handler."""

import sys
import typing

from sentence_transformers import CrossEncoder
from transformers.tokenization_utils import TruncationStrategy

from apache_beam.ml.inference.base import ModelHandler
from apache_beam.ml.inference.base import PredictionResult

from shared.model_utils import CROSS_ENCODER_MODEL_NAME

_MAX_SEQ_LEN_ = 2048
_MIN_FINAL_SCORE_WIGHT = 0.9


class CrossEncoderModelHandler(
        ModelHandler[typing.Tuple[str, typing.Sequence[typing.Dict[str, any]]],
                     PredictionResult, CrossEncoder]):
    """Semantic similarity Cross-Encoder Model Handler class."""

    def __init__(
        self,
        model_name: str = CROSS_ENCODER_MODEL_NAME,
    ):
        self._model_name = model_name

    def load_model(self) -> CrossEncoder:
        """Loads and initializes a model for processing."""
        cross_encoder = CrossEncoder(
            self._model_name,
            max_length=_MAX_SEQ_LEN_,
            tokenizer_args={
                "use_fast": False,
                "truncation": TruncationStrategy.DO_NOT_TRUNCATE
            },
        )
        return cross_encoder

    def run_inference(
        self,
        batch: typing.Sequence[typing.Tuple[str,
                                            typing.Sequence[typing.Dict[str,
                                                                        any]]]],
        model: CrossEncoder,
        inference_args: typing.Optional[typing.Dict[str, typing.Any]] = None
    ) -> typing.Iterable[PredictionResult]:
        """Runs inferences on a batch of text string pairs.

        Args:
          batch: A sequence of sentence pairs (Tuples)
            where first element is ad_group_id,
            and second element is a sequence of dictionaries
            with results of Bi-Encoder matches
            filtered on top_k and minimum scores.
          model: a CrossEncoder model
          inference_args: Any additional arguments for an inference.

        Returns:
          An Iterable of type PredictionResult where every result
            with `example` being an id of the second element
            and `inference` a sum of all scores grouped by the id.
        """

        del inference_args

        for ad_group in batch:
            ag_group_id = ad_group[0]
            ad_group_matches = ad_group[1]
            if len(ad_group_matches) < 2:
                ad_group_matches[0]["score"] = 1.0
                yield PredictionResult(ag_group_id, [ad_group_matches[0]])
                continue

            ad_group_text = ad_group_matches[0]["ad_group_text"]
            fullhiers = [h["fullhier"] for h in ad_group_matches]
            texts_pairs = [[ad_group_text, h] for h in fullhiers]

            cross_scores = model.predict(texts_pairs,
                                         convert_to_numpy=True,
                                         show_progress_bar=False)
            if hasattr(cross_scores[0], "__len__") and len(cross_scores[0]) > 1:
                label2id: dict = model.config.label2id
                labels = [l.upper() for l in label2id]
                idx = list(label2id.values())
                positive_index = 0
                if "ENTAILMENT" in labels:
                    positive_index = idx[labels.index("ENTAILMENT")]
                elif "POSITIVE" in labels:
                    positive_index = idx[labels.index("POSITIVE")]
                cross_scores = [float(m[positive_index]) for m in cross_scores]
            else:
                cross_scores = [float(m) for m in cross_scores]

            score_pairs = []
            min_score = 0
            for index, score in enumerate(cross_scores):
                if score < min_score:
                    min_score = score
                score_pairs.append((index, score))
            # Move score values to be positive
            # and multiply with the score from bi-encoder
            score_pairs = [
                (p[0], (p[1] - min_score) * ad_group_matches[index]["score"])
                for index, p in enumerate(score_pairs)
            ]
            max_score = max([p[1] for p in score_pairs])
            min_good_score = max_score * _MIN_FINAL_SCORE_WIGHT
            filtered_scores = [s for s in score_pairs if s[1] >= min_good_score]
            sorted_scores = sorted(filtered_scores,
                                   key=lambda s: s[1],
                                   reverse=True)
            scores_sum = sum([s[1] for s in sorted_scores])
            final_matches = []
            for s in sorted_scores:
                index = s[0]
                this_match = ad_group_matches[index]
                this_match["score"] = s[1] / scores_sum
                final_matches.append(this_match)
            yield PredictionResult(ag_group_id, final_matches)
