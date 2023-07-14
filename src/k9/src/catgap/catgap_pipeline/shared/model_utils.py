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
"""SBERT/Huggingface model utilities."""

from pathlib import Path
import typing

from sentence_transformers import util, __version__

BI_ENCODER_MODEL_NAME = (
    # 1B sentence pairs training set, 768-dimensional sentence vectorization
    "sentence-transformers/all-mpnet-base-v2")
CROSS_ENCODER_MODEL_NAME = "cross-encoder/nli-deberta-v3-base"

def get_cached_model_path(cache_folder: str, model_name: str) -> str:
    """Downloads (if not already downloaded) and caches ML model
        from Hugging Face Hub.

    Args:
        model_name (str): model name

    Returns:
        str: model directory path
    """
    model_path = Path(cache_folder).joinpath(
                        model_name.replace("/", "_"))
    if not model_path.is_dir():
        model_path = util.snapshot_download(
            model_name,
            cache_dir=f"{cache_folder}",
            library_name="sentence-transformers",
            library_version=__version__,
            ignore_files=[
                "flax_model.msgpack",
                "rust_model.ot",
                "tf_model.h5",
            ],
        )
    return model_path


def pre_cache_models(cache_folder: str,
                     models: typing.Union[str, typing.List[str]] = None):
    """Makes sure models are cached.

    Args:
        models (typing.Union[str, typing.List[str]]): model name(s)
    """
    if not models:
        models = [BI_ENCODER_MODEL_NAME, CROSS_ENCODER_MODEL_NAME]
    elif isinstance(models, str):
        models = [models]
    for m in models:
        get_cached_model_path(cache_folder, m)
