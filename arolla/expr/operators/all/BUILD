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

package(default_visibility = ["//visibility:public"])

licenses(["notice"])

cc_library(
    name = "all",
    local_defines = ["AROLLA_IMPLEMENTATION"],
    tags = ["keep_dep"],
    deps = [
        ":standard_cpp",
        ":standard_operator_package",
        "//arolla/serialization_codecs/generic/decoders",
    ],
)

cc_library(
    name = "standard_cpp",
    srcs = [
        "standard_cpp.cc",
    ],
    local_defines = ["AROLLA_IMPLEMENTATION"],
    deps = [
        "//arolla/expr/operators",
        "//arolla/expr/operators/strings",
        "//arolla/util",
        "//arolla/util:status_backport",
        "@com_google_absl//absl/status",
    ],
    alwayslink = 1,
)

# We use this target only in copybara/gerrit environment.
arolla_cc_embed_operator_package(
    name = "standard_operator_package_snapshot",
    srcs = [
        "standard.operator_package.pb2.snapshot",
    ],
    priority_name = "kRegisterExprOperatorsStandard",
    visibility = ["//visibility:private"],
)

alias(
    name = "standard_operator_package",
    actual = ":standard_operator_package_snapshot",
    visibility = ["//visibility:private"],
)
