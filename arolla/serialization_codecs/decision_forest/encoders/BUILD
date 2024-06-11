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

package(default_visibility = ["//visibility:public"])

licenses(["notice"])

cc_library(
    name = "encoders",
    srcs = [
        "decision_forest_encoder.cc",
    ],
    local_defines = ["AROLLA_IMPLEMENTATION"],
    deps = [
        "//arolla/decision_forest",
        "//arolla/decision_forest/expr_operator",
        "//arolla/decision_forest/split_conditions",
        "//arolla/expr",
        "//arolla/qtype",
        "//arolla/serialization_base",
        "//arolla/serialization_codecs:registry",
        "//arolla/serialization_codecs/decision_forest:codec_name",
        "//arolla/serialization_codecs/decision_forest:decision_forest_codec_cc_proto",
        "//arolla/util",
        "//arolla/util:status_backport",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
    ],
    alwayslink = True,
)
