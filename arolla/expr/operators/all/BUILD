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

load(
    "//arolla/codegen/operator_package:operator_package.bzl",
    "arolla_cc_embed_operator_package",
)

package(default_visibility = ["//visibility:private"])

licenses(["notice"])

cc_library(
    name = "all",
    srcs = ["all.cc"],
    local_defines = ["AROLLA_IMPLEMENTATION"],
    visibility = ["//visibility:public"],
    deps = [
        ":standard_operator_package",
        "//arolla/expr/operators",
        "//arolla/util",
        "//arolla/util:status_backport",
        "@com_google_absl//absl/status",
    ],
    alwayslink = True,
)

# Note: We use the lower-level arolla_cc_embed_operator_package() here instead
# of arolla_cc_operator_package() for additional control over the dependencies.
arolla_cc_embed_operator_package(
    name = "standard_operator_package",
    srcs = ["//py/arolla/operators/standard:operator_package.pb2"],
    arolla_initializer = {
        "name": "arolla_operators/standard:operator_package",
        "deps": ["arolla_operators/standard:bootstrap"],
    },
    deps = [
        "//arolla/expr/operators:bootstrap",
        "//arolla/qexpr/operators/bootstrap",
        "//arolla/serialization_codecs/generic/decoders",
    ],
)
