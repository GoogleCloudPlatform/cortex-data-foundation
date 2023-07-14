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
"""Semantic similarity Bi-Encoder Model Handler."""

import typing

from sentence_transformers import SentenceTransformer

from apache_beam.ml.inference.base import ModelHandler
from apache_beam.ml.inference.base import PredictionResult

from shared.model_utils import BI_ENCODER_MODEL_NAME

_MAX_SEQ_LEN_ = 2048


class BiEncoderModelHandler(ModelHandler[typing.Tuple[str,
                                                      str], PredictionResult,
                                         SentenceTransformer]):
    """Semantic similarity Bi-Encoder Model Handler class."""

    def __init__(
        self,
        model_name: str = BI_ENCODER_MODEL_NAME,
    ):
        self._model_name = model_name

    def load_model(self) -> SentenceTransformer:
        """Loads and initializes a model for processing."""
        model = SentenceTransformer(self._model_name)
        model.max_seq_length = _MAX_SEQ_LEN_
        return model

    def run_inference(
        self,
        batch: typing.Sequence[typing.Tuple[str, str]],
        model: SentenceTransformer,
        inference_args: typing.Optional[typing.Dict[str, typing.Any]] = None
    ) -> typing.Iterable[PredictionResult]:
        """Runs inferences on a batch of text strings.

        Args:
          batch: A sequence of tuples of strings as (id, text).
          model: a SentenceTransformer model
          inference_args: Any additional arguments for an inference.

        Returns:
          An Iterable of type PredictionResult
            with `example` being a tuple (id, text)
            and `inference` being text's embeddings vector.
        """

        del inference_args

        texts = [x[1] for x in batch]

        embeddings = model.encode(
            texts,
            convert_to_numpy=True,
            show_progress_bar=False,
            normalize_embeddings=True,
        )
        return [
            PredictionResult(arg, vector)
            for arg, vector in zip(batch, embeddings)
        ]
