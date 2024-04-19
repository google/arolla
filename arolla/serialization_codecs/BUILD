# Copyright 2024 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Allows registration of default codecs.

package(default_visibility = ["//visibility:public"])

licenses(["notice"])

cc_library(
    name = "all",
    local_defines = ["AROLLA_IMPLEMENTATION"],
    tags = ["keep_dep"],
    deps = [
        ":all_decoders",
        "//arolla/serialization_codecs/array/encoders",
        "//arolla/serialization_codecs/decision_forest/encoders",
        "//arolla/serialization_codecs/dense_array/encoders",
        "//arolla/serialization_codecs/dict/encoders",
        "//arolla/serialization_codecs/generic/encoders",
    ],
)

cc_library(
    name = "all_decoders",
    local_defines = ["AROLLA_IMPLEMENTATION"],
    tags = ["keep_dep"],
    deps = [
        "//arolla/serialization_codecs/array/decoders",
        "//arolla/serialization_codecs/decision_forest/decoders",
        "//arolla/serialization_codecs/dense_array/decoders",
        "//arolla/serialization_codecs/dict/decoders",
        "//arolla/serialization_codecs/generic/decoders",
    ],
)
